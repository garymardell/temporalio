require "../spec_helper"
require "../../src/temporalio"
require "./workflows/simple_completion_workflow"
require "./activities/retryable_activity"

# Integration tests for pipelined async workflow starts
# These tests verify correctness and fault tolerance of the async API

# Helper to create client
def create_client_for_pipeline
  Temporalio::Client.connect(
    "http://localhost:7234",
    namespace: "default"
  )
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
  sleep 100.milliseconds
  %worker
end

describe "Pipelined Async Workflow Starts" do
  
  describe "start_workflow_async" do
    it "starts a single workflow asynchronously" do
      client = create_client_for_pipeline
      task_queue = "async-single-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        # Start workflow async
        future = client.start_workflow_async(
          SimpleCompletionWorkflow.workflow_name,
          "AsyncTest",
          id: "async-single-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 10.seconds
        )
        
        # Future should not be ready immediately
        future.ready?.should be_false
        
        # Wait for handle
        handle = future.get
        handle.should be_a(Temporalio::Client::WorkflowHandle)
        
        # Get result
        result = handle.result
        result.should eq("Hello, AsyncTest!")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
    
    it "handles errors correctly in async workflow start" do
      client = create_client_for_pipeline
      task_queue = "async-error-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        workflow_id = "async-error-#{Random.rand(10000)}"
        
        # Start first workflow
        future1 = client.start_workflow_async(
          SimpleCompletionWorkflow.workflow_name,
          "First",
          id: workflow_id,
          task_queue: task_queue,
          execution_timeout: 10.seconds
        )
        
        handle1 = future1.get
        
        # Try to start duplicate (should fail)
        future2 = client.start_workflow_async(
          SimpleCompletionWorkflow.workflow_name,
          "Second",
          id: workflow_id,  # Same ID!
          task_queue: task_queue,
          execution_timeout: 10.seconds
        )
        
        # Error should be captured in future
        expect_raises(Temporalio::Ext::ExtError, /already running/) do
          future2.get
        end
        
        # First workflow should still complete successfully
        result1 = handle1.result
        result1.should eq("Hello, First!")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
    
    it "supports cancelling futures" do
      client = create_client_for_pipeline
      task_queue = "async-cancel-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        future = client.start_workflow_async(
          SimpleCompletionWorkflow.workflow_name,
          "CancelTest",
          id: "async-cancel-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 10.seconds
        )
        
        # Cancel the future before it completes
        future.cancel.should be_true
        
        # Attempting to get should raise
        expect_raises(Temporalio::CancelledError) do
          future.get
        end
        
        future.cancelled?.should be_true
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end
  
  describe "start_workflows_pipelined" do
    it "starts multiple workflows with pipelining" do
      client = create_client_for_pipeline
      task_queue = "pipelined-multi-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        num_workflows = 50
        
        # Build requests
        requests = num_workflows.times.map do |i|
          {
            workflow_type: SimpleCompletionWorkflow.workflow_name,
            args: ["Pipeline#{i}".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
            id: "pipelined-#{i}-#{Random.rand(10000)}",
            task_queue: task_queue
          }
        end.to_a
        
        # Start all workflows pipelined
        start_time = Time.monotonic
        futures = client.start_workflows_pipelined(requests)
        pipeline_time = (Time.monotonic - start_time).total_milliseconds
        
        # All futures should be returned immediately
        futures.size.should eq(num_workflows)
        
        # Pipeline time should be very fast (not waiting for responses)
        pipeline_time.should be < 100  # Should take < 100ms to send all requests
        
        # Wait for all handles
        results = Temporalio::Async.await_all_settled(futures)
        
        # All should succeed
        successes = results.count { |r| r[:success] }
        successes.should eq(num_workflows)
        
        # Get all workflow results
        handles = results.map { |r| r[:value].not_nil! }
        workflow_results = handles.map(&.result)
        
        # Verify results
        num_workflows.times do |i|
          workflow_results[i].should eq("Hello, Pipeline#{i}!")
        end
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
    
    it "handles partial failures gracefully" do
      client = create_client_for_pipeline
      task_queue = "pipelined-partial-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        # Create requests with a duplicate ID to cause one failure
        duplicate_id = "pipelined-dup-#{Random.rand(10000)}"
        
        requests = [
          {
            workflow_type: SimpleCompletionWorkflow.workflow_name,
            args: ["First".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
            id: "pipelined-good1-#{Random.rand(10000)}",
            task_queue: task_queue
          },
          {
            workflow_type: SimpleCompletionWorkflow.workflow_name,
            args: ["Duplicate1".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
            id: duplicate_id,
            task_queue: task_queue
          },
          {
            workflow_type: SimpleCompletionWorkflow.workflow_name,
            args: ["Second".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
            id: "pipelined-good2-#{Random.rand(10000)}",
            task_queue: task_queue
          },
          {
            workflow_type: SimpleCompletionWorkflow.workflow_name,
            args: ["Duplicate2".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
            id: duplicate_id,  # Duplicate!
            task_queue: task_queue
          }
        ]
        
        # Start all pipelined
        futures = client.start_workflows_pipelined(requests)
        
        # Wait for all (including failures)
        results = Temporalio::Async.await_all_settled(futures)
        
        # Should have 3 successes and 1 failure
        successes = results.count { |r| r[:success] }
        failures = results.count { |r| !r[:success] }
        
        successes.should eq(3)
        failures.should eq(1)
        
        # The failed one should be an ExtError about already running
        failed_result = results.find { |r| !r[:success] }
        failed_result.should_not be_nil
        error = failed_result.not_nil![:error]
        error.should be_a(Temporalio::Ext::ExtError)
        error.not_nil!.message.not_nil!.should contain("already running")
        
        # Successful workflows should complete
        successful_handles = results.select { |r| r[:success] }.map { |r| r[:value].not_nil! }
        successful_handles.each do |handle|
          result = handle.result
          result.should match(/Hello, .*!/)
        end
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
    
    it "maintains correctness under high concurrency" do
      client = create_client_for_pipeline
      task_queue = "pipelined-concurrent-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        num_workflows = 100
        
        # Build requests
        requests = num_workflows.times.map do |i|
          {
            workflow_type: SimpleCompletionWorkflow.workflow_name,
            args: ["Concurrent#{i}".as(String)].map(&.as(String | Int32 | Int64 | Bool | Nil)),
            id: "concurrent-#{i}-#{Random.rand(10000)}",
            task_queue: task_queue
          }
        end.to_a
        
        # Start all pipelined
        futures = client.start_workflows_pipelined(requests)
        
        # Wait for all
        results = Temporalio::Async.await_all_settled(futures)
        
        # All should succeed
        results.all? { |r| r[:success] }.should be_true
        
        # Verify each workflow produces correct result
        handles = results.map { |r| r[:value].not_nil! }
        workflow_results = handles.map(&.result)
        
        num_workflows.times do |i|
          workflow_results[i].should eq("Hello, Concurrent#{i}!")
        end
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
    
    it "supports timeout on future.get" do
      client = create_client_for_pipeline
      
      # Create a future that never completes
      future = Temporalio::Async::Future(String).new
      
      # Should timeout
      expect_raises(Temporalio::Error, /timed out/) do
        future.get(timeout: 100.milliseconds)
      end
      
      # Future should still not be ready
      future.ready?.should be_false
    end
  end
  
  describe "Async::Future helper methods" do
    it "supports Future.map for transformations" do
      client = create_client_for_pipeline
      task_queue = "async-map-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        future = client.start_workflow_async(
          SimpleCompletionWorkflow.workflow_name,
          "MapTest",
          id: "map-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 10.seconds
        )
        
        # Map to workflow_id
        id_future = future.map { |handle| handle.workflow_id }
        
        workflow_id = id_future.get
        workflow_id.should start_with("map-")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
    
    it "supports Async.await_all for waiting on multiple futures" do
      client = create_client_for_pipeline
      task_queue = "async-await-all-#{Random.rand(10000)}"
      
      worker = create_worker(
        client,
        task_queue,
        [SimpleCompletionWorkflow],
        [] of Temporalio::Activity
      )
      
      begin
        futures = 10.times.map do |i|
          client.start_workflow_async(
            SimpleCompletionWorkflow.workflow_name,
            "Batch#{i}",
            id: "batch-#{i}-#{Random.rand(10000)}",
            task_queue: task_queue,
            execution_timeout: 10.seconds
          )
        end.to_a
        
        # Wait for all
        handles = Temporalio::Async.await_all(futures)
        
        handles.size.should eq(10)
        handles.each { |h| h.should be_a(Temporalio::Client::WorkflowHandle) }
        
        # All should complete successfully
        results = handles.map(&.result)
        results.each_with_index do |result, i|
          result.should eq("Hello, Batch#{i}!")
        end
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end
end
