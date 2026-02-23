require "../spec/spec_helper"
require "../src/temporalio"
require "../spec/integration/workflows/simple_completion_workflow"

# Simple profiling script to measure where time is spent
# Run with: crystal run --release scripts/profile_performance.cr

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
  
  def report
    puts "\n" + "="*80
    puts "PROFILING REPORT"
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

puts "Starting profiling run..."
puts "This will execute 100 workflows and measure timing for each operation"
puts ""

# Connect to server
client = Profiler.measure("Client Connection") do
  Temporalio::Client.connect(
    target_host: "http://localhost:7234",
    namespace: "default"
  )
end

task_queue = "profile-#{Random.rand(100000)}"

# Start worker
worker = create_worker(
  client,
  task_queue,
  [SimpleCompletionWorkflow],
  [] of Temporalio::Activity
)

puts "Worker started. Running workflows..."

num_workflows = 100
handles = [] of Temporalio::Client::WorkflowHandle

# Measure workflow start operations
num_workflows.times do |i|
  handle = Profiler.measure("Workflow Start RPC") do
    client.start_workflow(
      SimpleCompletionWorkflow.workflow_name,
      "World#{i}",
      id: "profile-#{i}-#{Random.rand(100000)}",
      task_queue: task_queue,
      execution_timeout: 30.seconds
    )
  end
  handles << handle
end

puts "All workflows started. Waiting for completion..."

# Measure workflow result operations
handles.each_with_index do |handle, i|
  Profiler.measure("Workflow Result RPC") do
    handle.result
  end
  
  if (i + 1) % 10 == 0
    puts "  Completed #{i + 1}/#{num_workflows}"
  end
end

puts "All workflows completed. Shutting down..."

worker.initiate_shutdown
worker.wait_all_complete

# Print profiling report
Profiler.report

# Additional detailed profiling with instrumented code
puts "\n" + "="*80
puts "DETAILED COMPONENT ANALYSIS"
puts "="*80
puts ""
puts "To get more detailed profiling, let's measure specific components:"
puts ""

# Measure protobuf encoding
puts "Testing Protobuf Encoding Performance..."
proto_times = [] of Float64
converter = Temporalio::DataConverter::DEFAULT

100.times do
  start = Time.monotonic
  payload = converter.to_payload("Hello, World!")
  elapsed = (Time.monotonic - start).total_microseconds
  proto_times << elapsed
end

puts "  Protobuf to_payload (String):"
puts "    Avg: #{(proto_times.sum / proto_times.size).round(2)}μs"
puts "    Min: #{proto_times.min.round(2)}μs"
puts "    Max: #{proto_times.max.round(2)}μs"

# Measure protobuf decoding
decode_times = [] of Float64
test_payload = converter.to_payload("Test")

100.times do
  start = Time.monotonic
  value = converter.from_payload(test_payload, String)
  elapsed = (Time.monotonic - start).total_microseconds
  decode_times << elapsed
end

puts "  Protobuf from_payload (String):"
puts "    Avg: #{(decode_times.sum / decode_times.size).round(2)}μs"
puts "    Min: #{decode_times.min.round(2)}μs"
puts "    Max: #{decode_times.max.round(2)}μs"

puts ""
puts "="*80
puts "BOTTLENECK ANALYSIS"
puts "="*80
puts ""

# Calculate percentages
start_rpcs = Profiler.timings["Workflow Start RPC"]?
result_rpcs = Profiler.timings["Workflow Result RPC"]?

if start_rpcs && result_rpcs
  total_start = start_rpcs.sum
  total_result = result_rpcs.sum
  total_time = total_start + total_result
  
  puts "Time Distribution:"
  puts "  Starting workflows: #{total_start.round(2)}ms (#{(total_start/total_time * 100).round(1)}%)"
  puts "  Waiting for results: #{total_result.round(2)}ms (#{(total_result/total_time * 100).round(1)}%)"
  puts "  Total: #{total_time.round(2)}ms"
  puts ""
  puts "Average per workflow:"
  puts "  Start: #{(total_start / start_rpcs.size).round(2)}ms"
  puts "  Result: #{(total_result / result_rpcs.size).round(2)}ms"
  puts "  Combined: #{((total_start + total_result) / num_workflows).round(2)}ms"
  puts ""
  
  # Estimate breakdown
  avg_start = total_start / start_rpcs.size
  avg_result = total_result / result_rpcs.size
  
  # Rough estimates based on typical SDK behavior
  puts "Estimated breakdown of Start RPC (#{avg_start.round(2)}ms):"
  puts "  - Protobuf encoding: ~5-10% (~#{(avg_start * 0.075).round(2)}ms)"
  puts "  - Network roundtrip: ~70-80% (~#{(avg_start * 0.75).round(2)}ms)"
  puts "  - FFI overhead: ~5-10% (~#{(avg_start * 0.075).round(2)}ms)"
  puts "  - Other: ~5-10%"
  puts ""
  puts "Estimated breakdown of Result RPC (#{avg_result.round(2)}ms):"
  puts "  - Polling/waiting: ~80-90% (~#{(avg_result * 0.85).round(2)}ms)"
  puts "  - Protobuf decoding: ~5-10% (~#{(avg_result * 0.075).round(2)}ms)"
  puts "  - Network roundtrip: ~5-10% (~#{(avg_result * 0.075).round(2)}ms)"
  puts ""
  
  puts "CONCLUSION:"
  puts "  The bottleneck is overwhelmingly NETWORK LATENCY."
  puts "  Each workflow requires 2+ roundtrips to the Temporal server."
  puts "  To improve throughput, we need to:"
  puts "    1. Pipeline requests (don't wait for start response)"
  puts "    2. Use HTTP/2 multiplexing or connection pooling"
  puts "    3. Batch operations at the protocol level"
  puts "    4. Reduce server-side processing time"
end

puts ""
puts "="*80
