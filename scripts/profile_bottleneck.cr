require "../spec/spec_helper"
require "../src/temporalio"
require "../src/temporalio/client_async"
require "../spec/integration/workflows/simple_completion_workflow"

# Deep profiling to find bottlenecks in pipelined execution
# Run with: crystal run --release scripts/profile_bottleneck.cr

module DetailedProfiler
  extend self
  
  @@timings = Hash(String, Array(Float64)).new { |h, k| h[k] = [] of Float64 }
  @@counters = Hash(String, Int64).new(0_i64)
  
  def measure(label : String, &block)
    start = Time.monotonic
    result = yield
    elapsed = (Time.monotonic - start).total_microseconds
    @@timings[label] << elapsed
    result
  end
  
  def count(label : String)
    @@counters[label] += 1
  end
  
  def timings
    @@timings
  end
  
  def counters
    @@counters
  end
  
  def reset
    @@timings.clear
    @@counters.clear
  end
  
  def report(title : String)
    puts "\n" + "="*80
    puts title
    puts "="*80
    
    # Sort by total time descending
    sorted = @@timings.to_a.sort_by { |(label, times)| -times.sum }
    
    total_all = sorted.sum { |(_, times)| times.sum }
    
    sorted.each do |(label, times)|
      next if times.empty?
      
      total = times.sum
      count = times.size
      avg = total / count
      min = times.min
      max = times.max
      pct = (total / total_all * 100).round(1)
      
      # Convert to appropriate units
      if avg > 1000
        puts "\n#{label}:"
        puts "  Count: #{count}"
        puts "  Total: #{(total/1000).round(2)}ms (#{pct}%)"
        puts "  Avg:   #{(avg/1000).round(2)}ms"
        puts "  Min:   #{(min/1000).round(2)}ms"
        puts "  Max:   #{(max/1000).round(2)}ms"
      else
        puts "\n#{label}:"
        puts "  Count: #{count}"
        puts "  Total: #{total.round(0)}μs (#{pct}%)"
        puts "  Avg:   #{avg.round(2)}μs"
        puts "  Min:   #{min.round(2)}μs"
        puts "  Max:   #{max.round(2)}μs"
      end
    end
    
    unless @@counters.empty?
      puts "\n" + "-"*80
      puts "COUNTERS:"
      @@counters.each do |label, count|
        puts "  #{label}: #{count}"
      end
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
puts "DETAILED BOTTLENECK PROFILING"
puts "="*80
puts ""
puts "This will instrument every step of pipelined execution to find bottlenecks."
puts ""

# Connect to server
client = Temporalio::Client.connect(
  target_host: "http://localhost:7234",
  namespace: "default"
)

num_workflows = 100
task_queue = "profile-bottleneck-#{Random.rand(100000)}"

worker = create_worker(
  client,
  task_queue,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

puts "Starting profiling with #{num_workflows} workflows..."
puts ""

# ============================================================================
# DETAILED PIPELINED PROFILING
# ============================================================================

overall_start = Time.monotonic

# PHASE 1: Start workflows asynchronously
puts "[Phase 1] Starting workflows asynchronously..."
phase1_start = Time.monotonic

async_starts = [] of Temporalio::Client::AsyncWorkflowStart

num_workflows.times do |i|
  # Measure the time to spawn the fiber
  spawn_start = Time.monotonic
  
  async_start = client.start_workflow_async_channel(
    SimpleCompletionWorkflow.workflow_name,
    "World#{i}",
    id: "profile-bottleneck-#{i}-#{Random.rand(100000)}",
    task_queue: task_queue,
    execution_timeout: 30.seconds
  )
  
  spawn_time = (Time.monotonic - spawn_start).total_microseconds
  DetailedProfiler.measure("1. Spawn fiber for async start") { spawn_time }
  
  async_starts << async_start
  DetailedProfiler.count("Workflows started")
  
  if (i + 1) % 25 == 0
    puts "  Started: #{i + 1}/#{num_workflows}"
  end
end

phase1_duration = (Time.monotonic - phase1_start).total_milliseconds
puts "  Phase 1 completed in #{phase1_duration.round(2)}ms"
puts ""

# PHASE 2: Await handles
puts "[Phase 2] Awaiting workflow handles..."
phase2_start = Time.monotonic

handles = async_starts.map_with_index do |async_start, i|
  await_start = Time.monotonic
  
  handle = DetailedProfiler.measure("2. Await workflow handle") do
    async_start.await
  end
  
  DetailedProfiler.count("Handles received")
  
  if (i + 1) % 25 == 0
    puts "  Received: #{i + 1}/#{num_workflows}"
  end
  
  handle
end

phase2_duration = (Time.monotonic - phase2_start).total_milliseconds
puts "  Phase 2 completed in #{phase2_duration.round(2)}ms"
puts ""

# PHASE 3: Get results
puts "[Phase 3] Getting workflow results..."
phase3_start = Time.monotonic

results = handles.map_with_index do |handle, i|
  result_start = Time.monotonic
  
  result = DetailedProfiler.measure("3. Get workflow result") do
    handle.result
  end
  
  DetailedProfiler.count("Results received")
  
  if (i + 1) % 25 == 0
    puts "  Completed: #{i + 1}/#{num_workflows}"
  end
  
  result
end

phase3_duration = (Time.monotonic - phase3_start).total_milliseconds
puts "  Phase 3 completed in #{phase3_duration.round(2)}ms"
puts ""

overall_duration = (Time.monotonic - overall_start).total_seconds

# Shutdown
worker.initiate_shutdown
worker.wait_all_complete

# ============================================================================
# COMPONENT-LEVEL PROFILING
# ============================================================================

puts "\n" + "="*80
puts "COMPONENT-LEVEL PROFILING"
puts "="*80
puts ""

# Profile data converter performance
puts "Testing DataConverter performance..."
converter = Temporalio::DataConverter::DEFAULT

# Encoding
encode_times = [] of Float64
1000.times do
  start = Time.monotonic
  payload = converter.to_payload("Hello, World!")
  elapsed = (Time.monotonic - start).total_microseconds
  encode_times << elapsed
end

puts "  Protobuf Encoding (1000 iterations):"
puts "    Avg: #{(encode_times.sum / encode_times.size).round(2)}μs"
puts "    Min: #{encode_times.min.round(2)}μs"
puts "    Max: #{encode_times.max.round(2)}μs"

# Decoding
decode_times = [] of Float64
test_payload = converter.to_payload("Test")

1000.times do
  start = Time.monotonic
  value = converter.from_payload(test_payload, String)
  elapsed = (Time.monotonic - start).total_microseconds
  decode_times << elapsed
end

puts "  Protobuf Decoding (1000 iterations):"
puts "    Avg: #{(decode_times.sum / decode_times.size).round(2)}μs"
puts "    Min: #{decode_times.min.round(2)}μs"
puts "    Max: #{decode_times.max.round(2)}μs"
puts ""

# Profile fiber spawning overhead
puts "Testing Fiber spawn overhead..."
fiber_times = [] of Float64

1000.times do
  start = Time.monotonic
  ch = Channel(Int32).new(1)
  spawn do
    ch.send(42)
  end
  result = ch.receive
  elapsed = (Time.monotonic - start).total_microseconds
  fiber_times << elapsed
end

puts "  Fiber spawn + channel communication (1000 iterations):"
puts "    Avg: #{(fiber_times.sum / fiber_times.size).round(2)}μs"
puts "    Min: #{fiber_times.min.round(2)}μs"
puts "    Max: #{fiber_times.max.round(2)}μs"
puts ""

# ============================================================================
# REPORT
# ============================================================================

DetailedProfiler.report("WORKFLOW EXECUTION PROFILING")

puts "\n" + "="*80
puts "PHASE BREAKDOWN"
puts "="*80
puts ""
puts "Phase 1 (Spawn async starts):    #{phase1_duration.round(2)}ms"
puts "Phase 2 (Await handles):          #{phase2_duration.round(2)}ms"
puts "Phase 3 (Get results):            #{phase3_duration.round(2)}ms"
puts ""
puts "Total elapsed time:               #{overall_duration.round(2)}s"
puts "Throughput:                       #{(num_workflows / overall_duration).round(2)} workflows/sec"
puts ""

# Calculate where time is spent
phase1_pct = (phase1_duration / (overall_duration * 1000) * 100).round(1)
phase2_pct = (phase2_duration / (overall_duration * 1000) * 100).round(1)
phase3_pct = (phase3_duration / (overall_duration * 1000) * 100).round(1)

puts "Time distribution:"
puts "  Phase 1: #{phase1_pct}%"
puts "  Phase 2: #{phase2_pct}%"
puts "  Phase 3: #{phase3_pct}%"
puts ""

# ============================================================================
# BOTTLENECK ANALYSIS
# ============================================================================

puts "="*80
puts "BOTTLENECK ANALYSIS"
puts "="*80
puts ""

# Calculate averages
spawn_times = DetailedProfiler.timings["1. Spawn fiber for async start"]?
await_times = DetailedProfiler.timings["2. Await workflow handle"]?
result_times = DetailedProfiler.timings["3. Get workflow result"]?

if spawn_times && await_times && result_times
  avg_spawn = spawn_times.sum / spawn_times.size / 1000  # Convert to ms
  avg_await = await_times.sum / await_times.size / 1000
  avg_result = result_times.sum / result_times.size / 1000
  
  total_avg = avg_spawn + avg_await + avg_result
  
  puts "Average time per workflow:"
  puts "  1. Spawn async start:  #{avg_spawn.round(2)}ms (#{(avg_spawn/total_avg*100).round(1)}%)"
  puts "  2. Await handle:       #{avg_await.round(2)}ms (#{(avg_await/total_avg*100).round(1)}%)"
  puts "  3. Get result:         #{avg_result.round(2)}ms (#{(avg_result/total_avg*100).round(1)}%)"
  puts "  Total per workflow:    #{total_avg.round(2)}ms"
  puts ""
  
  # Identify bottleneck
  bottleneck = if avg_result > avg_await && avg_result > avg_spawn
    "Phase 3: Getting workflow results"
  elsif avg_await > avg_spawn
    "Phase 2: Awaiting workflow handles (RPC)"
  else
    "Phase 1: Spawning async starts"
  end
  
  puts "PRIMARY BOTTLENECK: #{bottleneck}"
  puts ""
  
  if avg_result > avg_await * 2
    puts "⚠ Workflow result phase is significantly slower than handle await phase."
    puts "  This suggests the bottleneck is in:"
    puts "    - Worker processing time (sequential workflow task processing)"
    puts "    - Server-side workflow execution"
    puts "    - History polling (GetWorkflowExecutionHistory RPC)"
  end
  
  if avg_await > avg_spawn * 10
    puts "⚠ Awaiting handles takes significantly longer than spawning fibers."
    puts "  This suggests the bottleneck is in:"
    puts "    - Network latency (StartWorkflowExecution RPC roundtrip)"
    puts "    - Server-side validation and persistence"
    puts "    - gRPC/HTTP overhead"
  end
  
  if avg_spawn > 1.0  # More than 1ms to spawn
    puts "⚠ Fiber spawning is taking significant time."
    puts "  This suggests overhead in Crystal's concurrency primitives."
  end
end

puts ""
puts "Component overhead:"
puts "  Protobuf encode:    #{(encode_times.sum / encode_times.size).round(2)}μs (negligible)"
puts "  Protobuf decode:    #{(decode_times.sum / decode_times.size).round(2)}μs (negligible)"
puts "  Fiber spawn:        #{(fiber_times.sum / fiber_times.size).round(2)}μs (negligible)"
puts ""

puts "="*80
puts "RECOMMENDATIONS"
puts "="*80
puts ""

if await_times && result_times
  avg_await_ms = await_times.sum / await_times.size / 1000
  avg_result_ms = result_times.sum / result_times.size / 1000
  
  if avg_await_ms > 5.0
    puts "1. NETWORK LATENCY (Await handles: #{avg_await_ms.round(2)}ms avg)"
    puts "   - Consider HTTP/2 multiplexing to reduce RPC overhead"
    puts "   - Consider connection pooling for HTTP/1.1"
    puts "   - Profile network stack (is localhost actually fast?)"
    puts ""
  end
  
  if avg_result_ms > 20.0
    puts "2. WORKFLOW EXECUTION (Get results: #{avg_result_ms.round(2)}ms avg)"
    puts "   - Worker processes workflow tasks sequentially (one at a time)"
    puts "   - Consider parallel workflow task processing"
    puts "   - Profile worker poll loop timing"
    puts "   - Check if server is the bottleneck (try external Temporal server)"
    puts ""
  end
  
  if avg_result_ms > avg_await_ms * 3
    puts "3. WORKER IS THE BOTTLENECK"
    puts "   - Result phase (#{avg_result_ms.round(2)}ms) >> Await phase (#{avg_await_ms.round(2)}ms)"
    puts "   - The worker cannot process workflow tasks fast enough"
    puts "   - Solutions:"
    puts "     a) Allow parallel workflow task processing (careful: determinism!)"
    puts "     b) Optimize workflow execution logic"
    puts "     c) Profile worker activation processing"
    puts ""
  end
end

puts "="*80
