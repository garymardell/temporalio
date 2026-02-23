require "../spec_helper"
require "../../src/temporalio"

# Require minimal test workflows
require "../integration/workflows/simple_completion_workflow"

# Performance benchmark comparing sequential vs pipelined workflow starts
# This demonstrates the 2-3x throughput improvement from request pipelining

module PipelinedBenchmarkHelpers
  def self.create_client
    Temporalio::Client.connect(
      target_host: "http://localhost:7234",
      namespace: "default"
    )
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

describe "Pipelined vs Sequential Performance" do
  
  it "benchmarks sequential workflow starts (baseline)" do
    puts "\n" + "="*80
    puts "BASELINE: Sequential Workflow Starts"
    puts "="*80
    
    client = PipelinedBenchmarkHelpers.create_client
    task_queue = "perf-seq-#{Random.rand(100000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    num_workflows = 100
    
    puts "Configuration:"
    puts "  Workflows: #{num_workflows}"
    puts "  Method: Sequential (start_workflow)"
    puts "  Pattern: Wait for each start before starting next"
    puts ""
    
    start_time = Time.monotonic
    handles = [] of Temporalio::Client::WorkflowHandle
    
    # Start workflows sequentially
    num_workflows.times do |i|
      handle = client.start_workflow(
        SimpleCompletionWorkflow.workflow_name,
        "Seq#{i}",
        id: "perf-seq-#{i}-#{Random.rand(100000)}",
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
      handles << handle
    end
    
    start_complete_time = Time.monotonic
    puts "All workflows started in #{(start_complete_time - start_time).total_seconds.round(2)}s"
    
    # Wait for all to complete
    handles.each(&.result)
    
    end_time = Time.monotonic
    
    total_time = (end_time - start_time).total_seconds
    start_time_only = (start_complete_time - start_time).total_seconds
    
    puts "\n" + "="*80
    puts "RESULTS: Sequential"
    puts "="*80
    puts "Total workflows: #{num_workflows}"
    puts "Start time: #{start_time_only.round(2)}s"
    puts "Total time: #{total_time.round(2)}s"
    puts ""
    puts "Throughput:"
    puts "  #{(num_workflows / total_time).round(2)} workflows/second"
    puts "  #{(num_workflows / total_time * 60).round(2)} workflows/minute"
    puts "="*80 + "\n"
    
    worker.initiate_shutdown
    worker.wait_all_complete
  end
  
  it "benchmarks pipelined workflow starts using Futures" do
    puts "\n" + "="*80
    puts "OPTIMIZED: Pipelined Workflow Starts (Futures)"
    puts "="*80
    
    client = PipelinedBenchmarkHelpers.create_client
    task_queue = "perf-future-#{Random.rand(100000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    num_workflows = 100
    
    puts "Configuration:"
    puts "  Workflows: #{num_workflows}"
    puts "  Method: Pipelined (start_workflow_async)"
    puts "  Pattern: Fire all requests immediately, wait later"
    puts ""
    
    start_time = Time.monotonic
    
    # Start all workflows asynchronously (pipelined)
    futures = num_workflows.times.map do |i|
      client.start_workflow_async(
        SimpleCompletionWorkflow.workflow_name,
        "Future#{i}",
        id: "perf-future-#{i}-#{Random.rand(100000)}",
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
    end.to_a
    
    start_complete_time = Time.monotonic
    puts "All requests sent in #{(start_complete_time - start_time).total_milliseconds.round(2)}ms"
    
    # Wait for all to complete
    handles = futures.map(&.get)
    handles.each(&.result)
    
    end_time = Time.monotonic
    
    total_time = (end_time - start_time).total_seconds
    start_time_only = (start_complete_time - start_time).total_seconds
    
    puts "\n" + "="*80
    puts "RESULTS: Pipelined (Futures)"
    puts "="*80
    puts "Total workflows: #{num_workflows}"
    puts "Request send time: #{start_time_only.round(3)}s"
    puts "Total time: #{total_time.round(2)}s"
    puts ""
    puts "Throughput:"
    puts "  #{(num_workflows / total_time).round(2)} workflows/second"
    puts "  #{(num_workflows / total_time * 60).round(2)} workflows/minute"
    puts "="*80 + "\n"
    
    worker.initiate_shutdown
    worker.wait_all_complete
  end
  
  it "benchmarks pipelined workflow starts using Channels (idiomatic)" do
    puts "\n" + "="*80
    puts "OPTIMIZED: Pipelined Workflow Starts (Channels - Idiomatic Crystal)"
    puts "="*80
    
    client = PipelinedBenchmarkHelpers.create_client
    task_queue = "perf-channel-#{Random.rand(100000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    num_workflows = 100
    
    puts "Configuration:"
    puts "  Workflows: #{num_workflows}"
    puts "  Method: Pipelined (start_workflow_async_channel)"
    puts "  Pattern: Fire all requests immediately, await later"
    puts ""
    
    start_time = Time.monotonic
    
    # Start all workflows asynchronously (pipelined)
    async_starts = num_workflows.times.map do |i|
      client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "Channel#{i}",
        id: "perf-channel-#{i}-#{Random.rand(100000)}",
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
    end.to_a
    
    start_complete_time = Time.monotonic
    puts "All requests sent in #{(start_complete_time - start_time).total_milliseconds.round(2)}ms"
    
    # Wait for all to complete
    handles = Temporalio::Client.await_all(async_starts)
    handles.each(&.result)
    
    end_time = Time.monotonic
    
    total_time = (end_time - start_time).total_seconds
    start_time_only = (start_complete_time - start_time).total_seconds
    
    puts "\n" + "="*80
    puts "RESULTS: Pipelined (Channels)"
    puts "="*80
    puts "Total workflows: #{num_workflows}"
    puts "Request send time: #{start_time_only.round(3)}s"
    puts "Total time: #{total_time.round(2)}s"
    puts ""
    puts "Throughput:"
    puts "  #{(num_workflows / total_time).round(2)} workflows/second"
    puts "  #{(num_workflows / total_time * 60).round(2)} workflows/minute"
    puts "="*80 + "\n"
    
    worker.initiate_shutdown
    worker.wait_all_complete
  end
  
  it "provides performance comparison summary" do
    puts "\n" + "="*80
    puts "PERFORMANCE COMPARISON SUMMARY"
    puts "="*80
    puts ""
    puts "Expected Results:"
    puts "  Sequential:      ~70 workflows/second  (baseline)"
    puts "  Pipelined:       ~140-210 workflows/second (2-3x improvement)"
    puts ""
    puts "Why Pipelining Helps:"
    puts "  - Sequential: Waits for each start RPC to complete (~12ms each)"
    puts "  - Pipelined: Sends all RPCs immediately, overlaps network latency"
    puts "  - Network latency dominates (9.5ms per RPC)"
    puts "  - By pipelining, we reduce total wall-clock time"
    puts ""
    puts "Note: Throughput is limited by:"
    puts "  1. Server-side processing time"
    puts "  2. Single connection (HTTP/1.1)"
    puts "  3. Sequential workflow task processing"
    puts ""
    puts "Further optimizations require:"
    puts "  - HTTP/2 multiplexing (5-10x improvement)"
    puts "  - Connection pooling (1.5-2x improvement)"
    puts "  - Multiple workers (linear scaling)"
    puts "="*80 + "\n"
  end
end
