require "../spec/spec_helper"
require "../src/temporalio"
require "../src/temporalio/client_async"
require "../spec/integration/workflows/simple_completion_workflow"

# Profiling script to compare sequential vs pipelined performance
# Run with: crystal run --release scripts/profile_pipelined.cr

module Profiler
  extend self
  
  @@timings = Hash(String, Array(Float64)).new { |h, k| h[k] = [] of Float64 }
  
  def measure(label : String, &block)
    start = Time.monotonic
    result = yield
    elapsed = (Time.monotonic - start).total_milliseconds
    @@timings[label] << elapsed
    result
  end
  
  def timings
    @@timings
  end
  
  def reset
    @@timings.clear
  end
  
  def report(title : String)
    puts "\n" + "="*80
    puts title
    puts "="*80
    
    @@timings.each do |label, times|
      next if times.empty?
      
      total = times.sum
      count = times.size
      avg = total / count
      min = times.min
      max = times.max
      
      puts "\n#{label}:"
      puts "  Count: #{count}"
      puts "  Total: #{total.round(2)}ms"
      puts "  Avg:   #{avg.round(2)}ms"
      puts "  Min:   #{min.round(2)}ms"
      puts "  Max:   #{max.round(2)}ms"
    end
    
    puts "\n" + "="*80
  end
end

# Macro to create worker
macro create_worker(client, task_queue, workflows, activities)
  %workflow_defs = Array(Temporalio::Internal::WorkflowDefinition).new
  {% for wf in workflows %}
    %workflow_defs << Temporalio::Internal::ConcreteWorkflowDefinition({{wf}}).new
  {% end %}
  
  %activity_defs = Array(Temporalio::Internal::ActivityDefinition).new
  {% for act in activities %}
    %activity_defs << Temporalio::Internal::ConcreteActivityDefinition({{act}}).new
  {% end %}
  
  %worker = Temporalio::Worker.new(
    client: {{client}},
    task_queue: {{task_queue}},
    workflows: %workflow_defs,
    activities: %activity_defs
  )
  
  spawn { %worker.run }
  sleep 200.milliseconds
  %worker
end

puts "="*80
puts "PERFORMANCE PROFILING: Sequential vs Pipelined"
puts "="*80
puts ""

# Connect to server
client = Temporalio::Client.connect(
  target_host: "http://localhost:7234",
  namespace: "default"
)

num_workflows = 100

# ============================================================================
# TEST 1: SEQUENTIAL (baseline)
# ============================================================================
puts "\n[1/3] Running SEQUENTIAL test (#{num_workflows} workflows)..."
puts "-"*80

task_queue_seq = "profile-seq-#{Random.rand(100000)}"
worker_seq = create_worker(
  client,
  task_queue_seq,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

Profiler.reset
sequential_start = Time.monotonic

num_workflows.times do |i|
  handle = Profiler.measure("Sequential: Start") do
    client.start_workflow(
      SimpleCompletionWorkflow.workflow_name,
      "World#{i}",
      id: "profile-seq-#{i}-#{Random.rand(100000)}",
      task_queue: task_queue_seq,
      execution_timeout: 30.seconds
    )
  end
  
  Profiler.measure("Sequential: Result") do
    handle.result
  end
  
  if (i + 1) % 25 == 0
    puts "  Progress: #{i + 1}/#{num_workflows}"
  end
end

sequential_total = (Time.monotonic - sequential_start).total_seconds
sequential_throughput = num_workflows / sequential_total

worker_seq.initiate_shutdown
worker_seq.wait_all_complete

Profiler.report("SEQUENTIAL RESULTS")

puts "\nSequential Summary:"
puts "  Total Time: #{sequential_total.round(2)}s"
puts "  Throughput: #{sequential_throughput.round(2)} workflows/sec"

# ============================================================================
# TEST 2: PIPELINED (start all, then await all)
# ============================================================================
puts "\n\n[2/3] Running PIPELINED test (#{num_workflows} workflows)..."
puts "-"*80

task_queue_pipe = "profile-pipe-#{Random.rand(100000)}"
worker_pipe = create_worker(
  client,
  task_queue_pipe,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

Profiler.reset
pipelined_start = Time.monotonic

# Phase 1: Start all workflows (pipelined - don't wait)
async_starts = [] of Temporalio::Client::AsyncWorkflowStart

start_phase_start = Time.monotonic
num_workflows.times do |i|
  async_start = Profiler.measure("Pipelined: Start (async)") do
    client.start_workflow_async_channel(
      SimpleCompletionWorkflow.workflow_name,
      "World#{i}",
      id: "profile-pipe-#{i}-#{Random.rand(100000)}",
      task_queue: task_queue_pipe,
      execution_timeout: 30.seconds
    )
  end
  async_starts << async_start
  
  if (i + 1) % 25 == 0
    puts "  Started: #{i + 1}/#{num_workflows}"
  end
end
start_phase_duration = (Time.monotonic - start_phase_start).total_milliseconds

# Phase 2: Await all handles
puts "  Awaiting handles..."
await_phase_start = Time.monotonic
handles = async_starts.map do |async_start|
  Profiler.measure("Pipelined: Await handle") do
    async_start.await
  end
end
await_phase_duration = (Time.monotonic - await_phase_start).total_milliseconds

# Phase 3: Get all results
puts "  Getting results..."
result_phase_start = Time.monotonic
handles.each_with_index do |handle, i|
  Profiler.measure("Pipelined: Result") do
    handle.result
  end
  
  if (i + 1) % 25 == 0
    puts "  Completed: #{i + 1}/#{num_workflows}"
  end
end
result_phase_duration = (Time.monotonic - result_phase_start).total_milliseconds

pipelined_total = (Time.monotonic - pipelined_start).total_seconds
pipelined_throughput = num_workflows / pipelined_total

worker_pipe.initiate_shutdown
worker_pipe.wait_all_complete

Profiler.report("PIPELINED RESULTS")

puts "\nPipelined Summary:"
puts "  Total Time: #{pipelined_total.round(2)}s"
puts "  Throughput: #{pipelined_throughput.round(2)} workflows/sec"
puts ""
puts "  Phase Breakdown:"
puts "    Start Phase:  #{start_phase_duration.round(2)}ms (launching async starts)"
puts "    Await Phase:  #{await_phase_duration.round(2)}ms (waiting for handles)"
puts "    Result Phase: #{result_phase_duration.round(2)}ms (waiting for completion)"

# ============================================================================
# TEST 3: FULLY PIPELINED (overlap start and completion)
# ============================================================================
puts "\n\n[3/3] Running FULLY PIPELINED test (#{num_workflows} workflows)..."
puts "-"*80
puts "  Strategy: Start in batches, immediately start awaiting results"

task_queue_full = "profile-full-#{Random.rand(100000)}"
worker_full = create_worker(
  client,
  task_queue_full,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

Profiler.reset
fully_pipelined_start = Time.monotonic

# Start and complete in overlapping batches
batch_size = 10
completed = 0

(num_workflows // batch_size).times do |batch_idx|
  batch_async = [] of Temporalio::Client::AsyncWorkflowStart
  
  # Start batch
  batch_size.times do |i|
    idx = batch_idx * batch_size + i
    async_start = Profiler.measure("FullPipe: Start (async)") do
      client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "World#{idx}",
        id: "profile-full-#{idx}-#{Random.rand(100000)}",
        task_queue: task_queue_full,
        execution_timeout: 30.seconds
      )
    end
    batch_async << async_start
  end
  
  # Await handles for this batch
  handles = batch_async.map do |async_start|
    Profiler.measure("FullPipe: Await handle") do
      async_start.await
    end
  end
  
  # Get results for this batch
  handles.each do |handle|
    Profiler.measure("FullPipe: Result") do
      handle.result
    end
    completed += 1
  end
  
  if (completed) % 25 == 0
    puts "  Progress: #{completed}/#{num_workflows}"
  end
end

fully_pipelined_total = (Time.monotonic - fully_pipelined_start).total_seconds
fully_pipelined_throughput = num_workflows / fully_pipelined_total

worker_full.initiate_shutdown
worker_full.wait_all_complete

Profiler.report("FULLY PIPELINED RESULTS")

puts "\nFully Pipelined Summary:"
puts "  Total Time: #{fully_pipelined_total.round(2)}s"
puts "  Throughput: #{fully_pipelined_throughput.round(2)} workflows/sec"

# ============================================================================
# FINAL COMPARISON
# ============================================================================
puts "\n\n" + "="*80
puts "FINAL COMPARISON"
puts "="*80
puts ""

puts "Throughput (workflows/sec):"
puts "  Sequential:        #{sequential_throughput.round(2)}"
puts "  Pipelined:         #{pipelined_throughput.round(2)}"
puts "  Fully Pipelined:   #{fully_pipelined_throughput.round(2)}"
puts ""

puts "Speedup vs Sequential:"
puts "  Pipelined:         #{(pipelined_throughput / sequential_throughput).round(2)}x"
puts "  Fully Pipelined:   #{(fully_pipelined_throughput / sequential_throughput).round(2)}x"
puts ""

puts "Total Time (seconds):"
puts "  Sequential:        #{sequential_total.round(2)}s"
puts "  Pipelined:         #{pipelined_total.round(2)}s"
puts "  Fully Pipelined:   #{fully_pipelined_total.round(2)}s"
puts ""

puts "="*80
puts "ANALYSIS"
puts "="*80
puts ""

if pipelined_throughput > sequential_throughput * 1.2
  puts "✓ Pipelining shows significant improvement (>20% faster)"
  puts "  The async approach is successfully overlapping network operations."
else
  puts "⚠ Pipelining shows minimal improvement"
  puts "  Possible causes:"
  puts "    - Worker processing is the bottleneck (sequential workflow task processing)"
  puts "    - Fiber spawning overhead negates benefits"
  puts "    - Network connection is already saturated"
  puts "    - Sample size too small to show statistical difference"
end
puts ""

if fully_pipelined_throughput > pipelined_throughput * 1.1
  puts "✓ Batch overlapping provides additional benefit"
  puts "  Starting the next batch while waiting for results helps throughput."
else
  puts "⚠ Batch overlapping doesn't help further"
  puts "  The bottleneck is likely in worker processing, not client operations."
end
puts ""

puts "="*80
