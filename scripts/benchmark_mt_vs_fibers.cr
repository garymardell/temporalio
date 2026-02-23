require "../spec/spec_helper"
require "../src/temporalio"
require "../src/temporalio/client_async"
require "../spec/integration/workflows/simple_completion_workflow"

# Benchmark: Multi-threading vs Fiber concurrency
# 
# This script compares performance with and without multi-threading.
# Note: This script must be compiled twice to get accurate comparison:
#
#   1. Without MT: crystal run --release scripts/benchmark_mt_vs_fibers.cr
#   2. With MT:    crystal run --release -Dpreview_mt scripts/benchmark_mt_vs_fibers.cr
#
# The script detects which mode it's running in and reports accordingly.

# Macro to create worker
macro create_worker(client, task_queue, workflows)
  %workflow_defs = Array(Temporalio::Internal::WorkflowDefinition).new
  {% for wf in workflows %}
    %workflow_defs << Temporalio::Internal::ConcreteWorkflowDefinition({{wf}}).new
  {% end %}
  
  %worker = Temporalio::Worker.new(
    client: {{client}},
    task_queue: {{task_queue}},
    workflows: %workflow_defs,
    activities: [] of Temporalio::Internal::ActivityDefinition
  )
  
  spawn { %worker.run }
  sleep 200.milliseconds
  %worker
end

puts "="*80
puts "MULTI-THREADING vs FIBER CONCURRENCY BENCHMARK"
puts "="*80
puts ""

mode = {% if flag?(:preview_mt) %}
  "Multi-threaded"
{% else %}
  "Fiber-based"
{% end %}

puts "✓ Running with #{mode} concurrency"
puts ""

target_host = ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234"
namespace = "default"
num_workflows = 100

# ============================================================================
# Test 1: Single connection with pipelined requests
# ============================================================================
puts "[1/3] Single connection + pipelined requests"
puts "-"*80

client_single = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_single = "bench-single-#{Random.rand(100000)}"
worker_single = create_worker(client_single, task_queue_single, [SimpleCompletionWorkflow])

single_start = Time.monotonic

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

puts "  Duration:   #{single_duration.round(2)}s"
puts "  Throughput: #{single_throughput.round(2)} workflows/sec"
puts ""

# ============================================================================
# Test 2: Connection pool (5 connections) with pipelined requests
# ============================================================================
puts "[2/3] Connection pool (5) + pipelined requests"
puts "-"*80

client_pool = Temporalio::Client.connect_with_pool(
  target_host: target_host,
  namespace: namespace,
  pool_size: 5,
  initial_pool_size: 2
)

client_worker = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_pool = "bench-pool-#{Random.rand(100000)}"
worker_pool = create_worker(client_worker, task_queue_pool, [SimpleCompletionWorkflow])

pool_start = Time.monotonic

async_starts = num_workflows.times.map do |i|
  client_pool.start_workflow_async_channel(
    SimpleCompletionWorkflow.workflow_name,
    "World#{i}",
    id: "pool-#{i}-#{Random.rand(100000)}",
    task_queue: task_queue_pool,
    execution_timeout: 30.seconds
  )
end.to_a

handles = async_starts.map(&.await)
results = handles.map(&.result)

pool_duration = (Time.monotonic - pool_start).total_seconds
pool_throughput = num_workflows / pool_duration

if stats = client_pool.pool_stats
  puts "  Pool stats: #{stats[:total]} total, #{stats[:idle]} idle"
end

worker_pool.initiate_shutdown
worker_pool.wait_all_complete

puts "  Duration:   #{pool_duration.round(2)}s"
puts "  Throughput: #{pool_throughput.round(2)} workflows/sec"
puts "  Speedup:    #{(pool_throughput / single_throughput).round(2)}x vs single"
puts ""

# ============================================================================
# Test 3: Explicit concurrent execution (spawn pattern)
# ============================================================================
puts "[3/3] Connection pool (10) + concurrent spawns"
puts "-"*80

client_pool10 = Temporalio::Client.connect_with_pool(
  target_host: target_host,
  namespace: namespace,
  pool_size: 10,
  initial_pool_size: 3
)

client_worker10 = Temporalio::Client.connect(
  target_host: target_host,
  namespace: namespace
)

task_queue_spawn = "bench-spawn-#{Random.rand(100000)}"
worker_spawn = create_worker(client_worker10, task_queue_spawn, [SimpleCompletionWorkflow])

spawn_start = Time.monotonic

# Use spawn to create explicit concurrency
# In fiber mode: all spawns use same thread
# In MT mode: spawns can use multiple threads
results_channel = Channel(String).new(num_workflows)

num_workflows.times do |i|
  spawn do
    begin
      handle = client_pool10.start_workflow(
        SimpleCompletionWorkflow.workflow_name,
        "World#{i}",
        id: "spawn-#{i}-#{Random.rand(100000)}",
        task_queue: task_queue_spawn,
        execution_timeout: 30.seconds
      )
      result = handle.result.as(String)
      results_channel.send(result)
    rescue ex
      puts "  Error in spawn #{i}: #{ex.message}"
      results_channel.send("")
    end
  end
end

# Collect all results
results = [] of String
num_workflows.times { results << results_channel.receive }

spawn_duration = (Time.monotonic - spawn_start).total_seconds
spawn_throughput = num_workflows / spawn_duration

if stats = client_pool10.pool_stats
  puts "  Pool stats: #{stats[:total]} total, #{stats[:idle]} idle"
end

worker_spawn.initiate_shutdown
worker_spawn.wait_all_complete

puts "  Duration:   #{spawn_duration.round(2)}s"
puts "  Throughput: #{spawn_throughput.round(2)} workflows/sec"
puts "  Speedup:    #{(spawn_throughput / single_throughput).round(2)}x vs single"
puts ""

# ============================================================================
# Summary
# ============================================================================
puts "="*80
puts "SUMMARY - #{mode}"
puts "="*80
puts ""

results_table = [
  {name: "Single conn + pipeline", throughput: single_throughput, speedup: 1.0},
  {name: "Pool(5) + pipeline", throughput: pool_throughput, speedup: pool_throughput / single_throughput},
  {name: "Pool(10) + spawn", throughput: spawn_throughput, speedup: spawn_throughput / single_throughput}
]

puts "%-25s %18s %15s" % ["Configuration", "Throughput", "Speedup"]
puts "-"*80
results_table.each do |result|
  puts "%-25s %15.2f/sec %14.2fx" % [result[:name], result[:throughput], result[:speedup]]
end

puts ""
puts "Concurrency model: #{mode}"
puts ""

{% if flag?(:preview_mt) %}
  puts "Multi-threading insights:"
  puts "  • Spawns can run on different OS threads"
  puts "  • Connection pool provides true parallel RPC calls"
  puts "  • Better CPU utilization on multi-core systems"
  puts ""
  puts "To compare with fiber mode, run:"
  puts "  crystal run --release scripts/benchmark_mt_vs_fibers.cr"
{% else %}
  puts "Fiber concurrency insights:"
  puts "  • All spawns run on single OS thread"
  puts "  • Cooperative multitasking (no preemption)"
  puts "  • Efficient for I/O-bound workloads"
  puts ""
  puts "To enable multi-threading, run:"
  puts "  crystal run --release -Dpreview_mt scripts/benchmark_mt_vs_fibers.cr"
{% end %}

puts ""
puts "="*80

# Save results to file for comparison
result_file = {% if flag?(:preview_mt) %}
  "benchmark_results_mt.txt"
{% else %}
  "benchmark_results_fibers.txt"
{% end %}

File.write(result_file, String.build do |io|
  io.puts "Mode: #{mode}"
  io.puts "Workflows: #{num_workflows}"
  io.puts ""
  io.puts "Results:"
  results_table.each do |result|
    io.puts "  #{result[:name]}: #{result[:throughput].round(2)} wf/sec (#{result[:speedup].round(2)}x)"
  end
end)

puts "Results saved to: #{result_file}"
puts ""
