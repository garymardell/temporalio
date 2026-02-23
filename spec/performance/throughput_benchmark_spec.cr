require "../spec_helper"
require "../../src/temporalio"

# Require minimal test workflows for performance testing
require "../integration/workflows/simple_completion_workflow"
require "../integration/workflows/activity_retry_workflow"
require "../integration/activities/retryable_activity"

# Performance benchmark - measures maximum throughput
# This test spawns many workflows concurrently and measures how fast they complete

module BenchmarkHelpers
  def self.create_client
    Temporalio::Client.connect(
      target_host: "http://localhost:7234",
      namespace: "default"
      # TODO: Re-enable FastDataConverter after making it compatible with Worker
      # data_converter: Temporalio::FastDataConverter::DEFAULT
    )
  end
end

# Macro to create worker with correct types
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
    activities: %activity_defs,
    max_concurrent_activities: 1000  # Allow high concurrency
  )
  
  spawn { %worker.run }
  sleep 200.milliseconds # Give worker time to start
  %worker
end

describe "Temporal SDK Performance Benchmark" do
  
  describe "Simple Workflow Throughput" do
    it "measures maximum workflows/second (simple completion - sequential)" do
      puts "\n" + "="*80
      puts "PERFORMANCE BENCHMARK: Simple Workflow Throughput (Sequential)"
      puts "="*80
      
      client = BenchmarkHelpers.create_client
      task_queue = "perf-simple-#{Random.rand(100000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      num_workflows = 1000
      batch_size = 100  # How many to start concurrently
      
      puts "Configuration:"
      puts "  Total workflows: #{num_workflows}"
      puts "  Batch size: #{batch_size}"
      puts "  Workflow type: SimpleCompletionWorkflow (no activities)"
      puts "  Mode: Sequential start (one at a time)"
      puts ""
      
      start_time = Time.monotonic
      completed = 0
      failed = 0
      
      # Track all workflow handles
      handles = [] of Temporalio::Client::WorkflowHandle
      
      puts "Starting workflows..."
      
      # Start all workflows as fast as possible
      num_workflows.times do |i|
        handle = client.start_workflow(
          SimpleCompletionWorkflow.workflow_name,
          "World#{i}",
          id: "perf-simple-#{i}-#{Random.rand(100000)}",
          task_queue: task_queue,
          execution_timeout: 30.seconds
        )
        handles << handle
        
        # Progress indicator
        if (i + 1) % 100 == 0
          elapsed = (Time.monotonic - start_time).total_seconds
          puts "  Started #{i + 1}/#{num_workflows} workflows (#{elapsed.round(2)}s)"
        end
      end
      
      start_complete_time = Time.monotonic
      puts "\nAll workflows started in #{(start_complete_time - start_time).total_seconds.round(2)}s"
      puts "Waiting for completion..."
      
      # Wait for all to complete
      handles.each_with_index do |handle, i|
        begin
          result = handle.result
          completed += 1
        rescue ex
          failed += 1
          puts "  Workflow #{i} failed: #{ex.message}"
        end
        
        # Progress indicator
        if (completed + failed) % 100 == 0
          elapsed = (Time.monotonic - start_complete_time).total_seconds
          puts "  Completed #{completed + failed}/#{num_workflows} workflows (#{elapsed.round(2)}s)"
        end
      end
      
      end_time = Time.monotonic
      
      total_time = (end_time - start_time).total_seconds
      execution_time = (end_time - start_complete_time).total_seconds
      
      puts "\n" + "="*80
      puts "RESULTS: Simple Workflow Throughput"
      puts "="*80
      puts "Total workflows: #{num_workflows}"
      puts "Completed: #{completed}"
      puts "Failed: #{failed}"
      puts "Success rate: #{(completed.to_f / num_workflows * 100).round(2)}%"
      puts ""
      puts "Timing:"
      puts "  Start time: #{(start_complete_time - start_time).total_seconds.round(2)}s"
      puts "  Execution time: #{execution_time.round(2)}s"
      puts "  Total time: #{total_time.round(2)}s"
      puts ""
      puts "Throughput (from start to completion):"
      puts "  #{(num_workflows / total_time).round(2)} workflows/second"
      puts "  #{(num_workflows / total_time * 60).round(2)} workflows/minute"
      puts ""
      puts "Throughput (execution only):"
      puts "  #{(num_workflows / execution_time).round(2)} workflows/second"
      puts "  #{(num_workflows / execution_time * 60).round(2)} workflows/minute"
      puts "="*80 + "\n"
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      completed.should be > (num_workflows * 0.95) # At least 95% success
    end

    it "measures maximum workflows/second (batch start - parallel)" do
      puts "\n" + "="*80
      puts "PERFORMANCE BENCHMARK: Simple Workflow Throughput (Batch Parallel)"
      puts "="*80
      
      client = BenchmarkHelpers.create_client
      task_queue = "perf-batch-#{Random.rand(100000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      num_workflows = 1000
      
      puts "Configuration:"
      puts "  Total workflows: #{num_workflows}"
      puts "  Workflow type: SimpleCompletionWorkflow (no activities)"
      puts "  Mode: Batch parallel start (all at once)"
      puts ""
      
      start_time = Time.monotonic
      
      puts "Starting #{num_workflows} workflows in batch mode..."
      
      # Build batch requests
      requests = num_workflows.times.map do |i|
        {
          workflow_type: SimpleCompletionWorkflow.workflow_name,
          args: ["World#{i}".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
          id: "perf-batch-#{i}-#{Random.rand(100000)}",
          task_queue: task_queue
        }
      end.to_a
      
      # Batch start all workflows
      handles = client.batch_start_workflows(requests)
      
      start_complete_time = Time.monotonic
      puts "All workflows started in #{(start_complete_time - start_time).total_seconds.round(2)}s"
      puts "Waiting for completion..."
      
      # Wait for all to complete
      completed = 0
      failed = 0
      
      handles.each_with_index do |handle, i|
        begin
          result = handle.result
          completed += 1
        rescue ex
          failed += 1
          puts "  Workflow #{i} failed: #{ex.message}"
        end
        
        # Progress indicator
        if (completed + failed) % 100 == 0
          elapsed = (Time.monotonic - start_complete_time).total_seconds
          puts "  Completed #{completed + failed}/#{num_workflows} workflows (#{elapsed.round(2)}s)"
        end
      end
      
      end_time = Time.monotonic
      
      total_time = (end_time - start_time).total_seconds
      execution_time = (end_time - start_complete_time).total_seconds
      
      puts "\n" + "="*80
      puts "RESULTS: Batch Parallel Workflow Throughput"
      puts "="*80
      puts "Total workflows: #{num_workflows}"
      puts "Completed: #{completed}"
      puts "Failed: #{failed}"
      puts "Success rate: #{(completed.to_f / num_workflows * 100).round(2)}%"
      puts ""
      puts "Timing:"
      puts "  Start time: #{(start_complete_time - start_time).total_seconds.round(2)}s"
      puts "  Execution time: #{execution_time.round(2)}s"
      puts "  Total time: #{total_time.round(2)}s"
      puts ""
      puts "Throughput (from start to completion):"
      puts "  #{(num_workflows / total_time).round(2)} workflows/second"
      puts "  #{(num_workflows / total_time * 60).round(2)} workflows/minute"
      puts ""
      puts "Throughput (execution only):"
      puts "  #{(num_workflows / execution_time).round(2)} workflows/second"
      puts "  #{(num_workflows / execution_time * 60).round(2)} workflows/minute"
      puts ""
      puts "SPEEDUP vs Sequential:"
      puts "  Batch parallel is faster by reducing start overhead"
      puts "="*80 + "\n"
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      completed.should be > (num_workflows * 0.95) # At least 95% success
    end
  end
  
  describe "Activity Workflow Throughput" do
    it "measures maximum workflows/second (with activities)" do
      puts "\n" + "="*80
      puts "PERFORMANCE BENCHMARK: Activity Workflow Throughput"
      puts "="*80
      
      client = BenchmarkHelpers.create_client
      task_queue = "perf-activity-#{Random.rand(100000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [ActivityRetryWorkflow],
        [RetryableActivity]
      )
      
      num_workflows = 500
      max_attempts = 1_i64  # No retries for performance test
      
      puts "Configuration:"
      puts "  Total workflows: #{num_workflows}"
      puts "  Workflow type: ActivityRetryWorkflow (3 activities each)"
      puts "  Max attempts: #{max_attempts}"
      puts ""
      
      start_time = Time.monotonic
      completed = 0
      failed = 0
      total_activities = num_workflows * 3
      
      # Track all workflow handles
      handles = [] of Temporalio::Client::WorkflowHandle
      
      puts "Starting workflows..."
      
      # Start all workflows as fast as possible
      num_workflows.times do |i|
        handle = client.start_workflow(
          ActivityRetryWorkflow.workflow_name,
          max_attempts,
          id: "perf-activity-#{i}-#{Random.rand(100000)}",
          task_queue: task_queue,
          execution_timeout: 60.seconds
        )
        handles << handle
        
        # Progress indicator
        if (i + 1) % 50 == 0
          elapsed = (Time.monotonic - start_time).total_seconds
          puts "  Started #{i + 1}/#{num_workflows} workflows (#{elapsed.round(2)}s)"
        end
      end
      
      start_complete_time = Time.monotonic
      puts "\nAll workflows started in #{(start_complete_time - start_time).total_seconds.round(2)}s"
      puts "Waiting for completion..."
      
      # Wait for all to complete
      handles.each_with_index do |handle, i|
        begin
          result = handle.result
          completed += 1
        rescue ex
          failed += 1
          puts "  Workflow #{i} failed: #{ex.message}"
        end
        
        # Progress indicator
        if (completed + failed) % 50 == 0
          elapsed = (Time.monotonic - start_complete_time).total_seconds
          puts "  Completed #{completed + failed}/#{num_workflows} workflows (#{elapsed.round(2)}s)"
        end
      end
      
      end_time = Time.monotonic
      
      total_time = (end_time - start_time).total_seconds
      execution_time = (end_time - start_complete_time).total_seconds
      
      puts "\n" + "="*80
      puts "RESULTS: Activity Workflow Throughput"
      puts "="*80
      puts "Total workflows: #{num_workflows}"
      puts "Total activities: #{total_activities}"
      puts "Completed: #{completed}"
      puts "Failed: #{failed}"
      puts "Success rate: #{(completed.to_f / num_workflows * 100).round(2)}%"
      puts ""
      puts "Timing:"
      puts "  Start time: #{(start_complete_time - start_time).total_seconds.round(2)}s"
      puts "  Execution time: #{execution_time.round(2)}s"
      puts "  Total time: #{total_time.round(2)}s"
      puts ""
      puts "Workflow Throughput (from start to completion):"
      puts "  #{(num_workflows / total_time).round(2)} workflows/second"
      puts "  #{(num_workflows / total_time * 60).round(2)} workflows/minute"
      puts ""
      puts "Workflow Throughput (execution only):"
      puts "  #{(num_workflows / execution_time).round(2)} workflows/second"
      puts "  #{(num_workflows / execution_time * 60).round(2)} workflows/minute"
      puts ""
      puts "Activity Throughput (execution only):"
      puts "  #{(total_activities / execution_time).round(2)} activities/second"
      puts "  #{(total_activities / execution_time * 60).round(2)} activities/minute"
      puts "="*80 + "\n"
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      completed.should be > (num_workflows * 0.95) # At least 95% success
    end
  end
  
  describe "Concurrent Execution Benchmark" do
    it "measures peak concurrent workflow execution" do
      puts "\n" + "="*80
      puts "PERFORMANCE BENCHMARK: Concurrent Execution Capacity"
      puts "="*80
      
      client = BenchmarkHelpers.create_client
      task_queue = "perf-concurrent-#{Random.rand(100000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      # Start many workflows and complete them all at once
      num_concurrent = 100
      
      puts "Configuration:"
      puts "  Concurrent workflows: #{num_concurrent}"
      puts "  Pattern: Start all, then wait for all"
      puts ""
      
      start_time = Time.monotonic
      
      puts "Starting #{num_concurrent} workflows concurrently..."
      
      # Start all workflows
      handles = (0...num_concurrent).map do |i|
        client.start_workflow(
          SimpleCompletionWorkflow.workflow_name,
          "Concurrent#{i}",
          id: "perf-concurrent-#{i}-#{Random.rand(100000)}",
          task_queue: task_queue,
          execution_timeout: 30.seconds
        )
      end
      
      start_complete_time = Time.monotonic
      puts "All workflows started in #{(start_complete_time - start_time).total_seconds.round(3)}s"
      
      puts "Waiting for all to complete..."
      
      # Wait for all concurrently (spawn fibers)
      channel = Channel(Bool).new(num_concurrent)
      
      handles.each do |handle|
        spawn do
          begin
            handle.result
            channel.send(true)
          rescue
            channel.send(false)
          end
        end
      end
      
      # Collect results
      completed = 0
      failed = 0
      num_concurrent.times do
        if channel.receive
          completed += 1
        else
          failed += 1
        end
      end
      
      end_time = Time.monotonic
      
      total_time = (end_time - start_time).total_seconds
      execution_time = (end_time - start_complete_time).total_seconds
      
      puts "\n" + "="*80
      puts "RESULTS: Concurrent Execution Capacity"
      puts "="*80
      puts "Concurrent workflows: #{num_concurrent}"
      puts "Completed: #{completed}"
      puts "Failed: #{failed}"
      puts "Success rate: #{(completed.to_f / num_concurrent * 100).round(2)}%"
      puts ""
      puts "Timing:"
      puts "  Start time: #{(start_complete_time - start_time).total_seconds.round(3)}s"
      puts "  Execution time: #{execution_time.round(3)}s"
      puts "  Total time: #{total_time.round(3)}s"
      puts ""
      puts "Average latency per workflow: #{(execution_time / num_concurrent * 1000).round(2)}ms"
      puts "Throughput: #{(num_concurrent / execution_time).round(2)} workflows/second"
      puts "="*80 + "\n"
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      completed.should eq(num_concurrent)
    end
  end
  
  describe "Latency Percentiles" do
    it "measures latency distribution (p50, p95, p99)" do
      puts "\n" + "="*80
      puts "PERFORMANCE BENCHMARK: Latency Percentiles"
      puts "="*80
      
      client = BenchmarkHelpers.create_client
      task_queue = "perf-latency-#{Random.rand(100000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      num_samples = 200
      
      puts "Configuration:"
      puts "  Sample size: #{num_samples}"
      puts "  Pattern: Measure end-to-end latency per workflow"
      puts ""
      
      latencies = [] of Float64
      
      puts "Measuring latencies..."
      
      num_samples.times do |i|
        workflow_start = Time.monotonic
        
        result = client.execute_workflow(
          SimpleCompletionWorkflow.workflow_name,
          "Latency#{i}",
          id: "perf-latency-#{i}-#{Random.rand(100000)}",
          task_queue: task_queue,
          execution_timeout: 30.seconds
        )
        
        workflow_end = Time.monotonic
        latency_ms = (workflow_end - workflow_start).total_milliseconds
        latencies << latency_ms
        
        if (i + 1) % 20 == 0
          puts "  Completed #{i + 1}/#{num_samples} samples"
        end
      end
      
      # Sort for percentile calculation
      latencies.sort!
      
      # Calculate percentiles
      p50_idx = (num_samples * 0.50).to_i
      p95_idx = (num_samples * 0.95).to_i
      p99_idx = (num_samples * 0.99).to_i
      
      p50 = latencies[p50_idx]
      p95 = latencies[p95_idx]
      p99 = latencies[p99_idx]
      min = latencies.first
      max = latencies.last
      avg = latencies.sum / latencies.size
      
      puts "\n" + "="*80
      puts "RESULTS: Latency Percentiles"
      puts "="*80
      puts "Sample size: #{num_samples}"
      puts ""
      puts "Latency (milliseconds):"
      puts "  Min:  #{min.round(2)}ms"
      puts "  p50:  #{p50.round(2)}ms"
      puts "  p95:  #{p95.round(2)}ms"
      puts "  p99:  #{p99.round(2)}ms"
      puts "  Max:  #{max.round(2)}ms"
      puts "  Avg:  #{avg.round(2)}ms"
      puts ""
      puts "Throughput (based on average):"
      puts "  #{(1000.0 / avg).round(2)} workflows/second"
      puts "  #{(60000.0 / avg).round(2)} workflows/minute"
      puts "="*80 + "\n"
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      # Sanity check - p99 should be reasonable
      p99.should be < 5000 # Less than 5 seconds
    end
  end
end
