require "../spec/spec_helper"
require "../src/temporalio"
require "../src/temporalio/client_async"
require "../spec/integration/workflows/simple_completion_workflow"

# Benchmark connection pooling vs single connection
# Run with: crystal run --release scripts/benchmark_pooling.cr

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
puts "CONNECTION POOLING PERFORMANCE BENCHMARK"
puts "="*80
puts ""

num_workflows = 100
target_host = "http://localhost:7234"
namespace = "default"

# ============================================================================
# TEST 1: Single connection (baseline)
# ============================================================================
puts "[1/4] Single connection (baseline)..."
puts "-"*80

client_single = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_single = "benchmark-single-#{Random.rand(100000)}"
worker_single = create_worker(
  client_single,
  task_queue_single,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

single_start = Time.monotonic

# Pipelined execution
async_starts = num_workflows.times.map do |i|
  client_single.start_workflow_async_channel(
    SimpleCompletionWorkflow.workflow_name,
    "World#{i}",
    id: "single-#{i}-#{Random.rand(100000)}",
    task_queue: task_queue_single,
    execution_timeout: 30.seconds
  )
end.to_a

handles = async_starts.map(&.await)
results = handles.map(&.result)

single_duration = (Time.monotonic - single_start).total_seconds
single_throughput = num_workflows / single_duration

worker_single.initiate_shutdown
worker_single.wait_all_complete

puts "  Completed: #{num_workflows} workflows"
puts "  Duration:  #{single_duration.round(2)}s"
puts "  Throughput: #{single_throughput.round(2)} workflows/sec"
puts ""

# ============================================================================
# TEST 2: Connection pool (3 connections)
# ============================================================================
puts "[2/4] Connection pool (3 connections)..."
puts "-"*80

# Note: Workers need non-pooled clients for long-lived polling
# We use a pooled client for starting workflows, but a separate non-pooled client for the worker
client_pool3 = Temporalio::Client.connect_with_pool(
  target_host: target_host,
  namespace: namespace,
  pool_size: 3,
  initial_pool_size: 1
)

client_worker3 = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_pool3 = "benchmark-pool3-#{Random.rand(100000)}"
worker_pool3 = create_worker(
  client_worker3,
  task_queue_pool3,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

pool3_start = Time.monotonic

async_starts = num_workflows.times.map do |i|
  client_pool3.start_workflow_async_channel(
    SimpleCompletionWorkflow.workflow_name,
    "World#{i}",
    id: "pool3-#{i}-#{Random.rand(100000)}",
    task_queue: task_queue_pool3,
    execution_timeout: 30.seconds
  )
end.to_a

# Check pool stats during execution
if stats = client_pool3.pool_stats
  puts "  Pool stats (during execution): total=#{stats[:total]}, idle=#{stats[:idle]}, in_use=#{stats[:in_use]}"
end

handles = async_starts.map(&.await)
results = handles.map(&.result)

pool3_duration = (Time.monotonic - pool3_start).total_seconds
pool3_throughput = num_workflows / pool3_duration

worker_pool3.initiate_shutdown
worker_pool3.wait_all_complete

if stats = client_pool3.pool_stats
  puts "  Pool stats (after completion): total=#{stats[:total]}, idle=#{stats[:idle]}, in_use=#{stats[:in_use]}"
end

puts "  Completed: #{num_workflows} workflows"
puts "  Duration:  #{pool3_duration.round(2)}s"
puts "  Throughput: #{pool3_throughput.round(2)} workflows/sec"
puts "  Speedup vs single: #{(pool3_throughput / single_throughput).round(2)}x"
puts ""

# ============================================================================
# TEST 3: Connection pool (5 connections)
# ============================================================================
puts "[3/4] Connection pool (5 connections)..."
puts "-"*80

client_pool5 = Temporalio::Client.connect_with_pool(
  target_host: target_host,
  namespace: namespace,
  pool_size: 5,
  initial_pool_size: 1
)

client_worker5 = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_pool5 = "benchmark-pool5-#{Random.rand(100000)}"
worker_pool5 = create_worker(
  client_worker5,
  task_queue_pool5,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

pool5_start = Time.monotonic

async_starts = num_workflows.times.map do |i|
  client_pool5.start_workflow_async_channel(
    SimpleCompletionWorkflow.workflow_name,
    "World#{i}",
    id: "pool5-#{i}-#{Random.rand(100000)}",
    task_queue: task_queue_pool5,
    execution_timeout: 30.seconds
  )
end.to_a

if stats = client_pool5.pool_stats
  puts "  Pool stats (during execution): total=#{stats[:total]}, idle=#{stats[:idle]}, in_use=#{stats[:in_use]}"
end

handles = async_starts.map(&.await)
results = handles.map(&.result)

pool5_duration = (Time.monotonic - pool5_start).total_seconds
pool5_throughput = num_workflows / pool5_duration

worker_pool5.initiate_shutdown
worker_pool5.wait_all_complete

if stats = client_pool5.pool_stats
  puts "  Pool stats (after completion): total=#{stats[:total]}, idle=#{stats[:idle]}, in_use=#{stats[:in_use]}"
end

puts "  Completed: #{num_workflows} workflows"
puts "  Duration:  #{pool5_duration.round(2)}s"
puts "  Throughput: #{pool5_throughput.round(2)} workflows/sec"
puts "  Speedup vs single: #{(pool5_throughput / single_throughput).round(2)}x"
puts ""

# ============================================================================
# TEST 4: Connection pool (10 connections)
# ============================================================================
puts "[4/4] Connection pool (10 connections)..."
puts "-"*80

client_pool10 = Temporalio::Client.connect_with_pool(
  target_host: target_host,
  namespace: namespace,
  pool_size: 10,
  initial_pool_size: 2
)

client_worker10 = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_pool10 = "benchmark-pool10-#{Random.rand(100000)}"
worker_pool10 = create_worker(
  client_worker10,
  task_queue_pool10,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

pool10_start = Time.monotonic

async_starts = num_workflows.times.map do |i|
  client_pool10.start_workflow_async_channel(
    SimpleCompletionWorkflow.workflow_name,
    "World#{i}",
    id: "pool10-#{i}-#{Random.rand(100000)}",
    task_queue: task_queue_pool10,
    execution_timeout: 30.seconds
  )
end.to_a

if stats = client_pool10.pool_stats
  puts "  Pool stats (during execution): total=#{stats[:total]}, idle=#{stats[:idle]}, in_use=#{stats[:in_use]}"
end

handles = async_starts.map(&.await)
results = handles.map(&.result)

pool10_duration = (Time.monotonic - pool10_start).total_seconds
pool10_throughput = num_workflows / pool10_duration

worker_pool10.initiate_shutdown
worker_pool10.wait_all_complete

if stats = client_pool10.pool_stats
  puts "  Pool stats (after completion): total=#{stats[:total]}, idle=#{stats[:idle]}, in_use=#{stats[:in_use]}"
end

puts "  Completed: #{num_workflows} workflows"
puts "  Duration:  #{pool10_duration.round(2)}s"
puts "  Throughput: #{pool10_throughput.round(2)} workflows/sec"
puts "  Speedup vs single: #{(pool10_throughput / single_throughput).round(2)}x"
puts ""

# ============================================================================
# FINAL SUMMARY
# ============================================================================
puts "="*80
puts "FINAL RESULTS"
puts "="*80
puts ""

results_table = [
  {name: "Single connection", throughput: single_throughput, speedup: 1.0},
  {name: "Pool (3 conns)", throughput: pool3_throughput, speedup: pool3_throughput / single_throughput},
  {name: "Pool (5 conns)", throughput: pool5_throughput, speedup: pool5_throughput / single_throughput},
  {name: "Pool (10 conns)", throughput: pool10_throughput, speedup: pool10_throughput / single_throughput}
]

puts "%-20s %15s %15s" % ["Configuration", "Throughput", "Speedup"]
puts "-"*80
results_table.each do |result|
  puts "%-20s %12.2f/sec %14.2fx" % [result[:name], result[:throughput], result[:speedup]]
end

puts ""
puts "="*80
puts "ANALYSIS"
puts "="*80
puts ""

best = results_table.max_by { |r| r[:throughput] }
puts "Best performance: #{best[:name]}"
puts "  Throughput: #{best[:throughput].round(2)} workflows/sec"
puts "  Speedup: #{best[:speedup].round(2)}x vs single connection"
puts ""

if pool10_throughput > pool5_throughput * 1.1
  puts "✓ Pool size 10 shows significant improvement over size 5"
  puts "  Consider increasing pool size further for even better performance."
elsif pool5_throughput > pool3_throughput * 1.1
  puts "✓ Pool size 5 is optimal - diminishing returns beyond this"
else
  puts "⚠ Pool size has minimal impact"
  puts "  The bottleneck is likely elsewhere (worker processing, server-side)."
end

puts ""
puts "Efficiency analysis:"
ideal_speedup3 = 3.0
ideal_speedup5 = 5.0
ideal_speedup10 = 10.0

efficiency3 = (pool3_throughput / single_throughput) / ideal_speedup3 * 100
efficiency5 = (pool5_throughput / single_throughput) / ideal_speedup5 * 100
efficiency10 = (pool10_throughput / single_throughput) / ideal_speedup10 * 100

puts "  Pool (3):  %.1f%% efficiency (%.2fx actual vs %.0fx ideal)" % [efficiency3, pool3_throughput / single_throughput, ideal_speedup3]
puts "  Pool (5):  %.1f%% efficiency (%.2fx actual vs %.0fx ideal)" % [efficiency5, pool5_throughput / single_throughput, ideal_speedup5]
puts "  Pool (10): %.1f%% efficiency (%.2fx actual vs %.0fx ideal)" % [efficiency10, pool10_throughput / single_throughput, ideal_speedup10]
puts ""

if efficiency5 < 50
  puts "⚠ Low efficiency suggests connection pooling is not the primary bottleneck."
  puts "  Other factors limiting performance:"
  puts "    - Sequential worker task processing"
  puts "    - Server-side processing latency"
  puts "    - Network protocol limitations (HTTP/1.1)"
else
  puts "✓ Good efficiency - connection pooling is effectively improving throughput."
end

puts ""
puts "="*80
