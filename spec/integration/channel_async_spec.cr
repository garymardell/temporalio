require "../spec_helper"
require "../../src/temporalio"
require "./workflows/simple_completion_workflow"

# Integration tests for idiomatic Crystal channel-based async API
# This tests the recommended pattern for Crystal developers

def create_client_for_channel
  Temporalio::Client.connect(
    "http://localhost:7234",
    namespace: "default"
  )
end

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

describe "Channel-Based Async API (Idiomatic Crystal)" do
  
  it "starts a workflow asynchronously using channels" do
    client = create_client_for_channel
    task_queue = "channel-single-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      # Start async
      async_start = client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "ChannelTest",
        id: "channel-single-#{Random.rand(10000)}",
        task_queue: task_queue,
        execution_timeout: 10.seconds
      )
      
      # Should not be ready immediately
      async_start.ready?.should be_false
      
      # Await completion
      handle = async_start.await
      handle.workflow_id.should start_with("channel-single")
      
      # Get result
      result = handle.result
      result.should eq("Hello, ChannelTest!")
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
  
  it "supports try_await for non-blocking checks" do
    client = create_client_for_channel
    task_queue = "channel-try-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      async_start = client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "TryTest",
        id: "channel-try-#{Random.rand(10000)}",
        task_queue: task_queue,
        execution_timeout: 10.seconds
      )
      
      # Try immediately (should return nil, not ready yet)
      result = async_start.try_await
      result.should be_nil
      
      # Wait a bit
      sleep 200.milliseconds
      
      # Try again (might be ready now)
      handle = async_start.try_await
      if handle
        handle.should be_a(Temporalio::Client::WorkflowHandle)
      else
        # If still not ready, await will work
        handle = async_start.await
        handle.should be_a(Temporalio::Client::WorkflowHandle)
      end
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
  
  it "handles errors correctly" do
    client = create_client_for_channel
    task_queue = "channel-error-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      workflow_id = "channel-dup-#{Random.rand(10000)}"
      
      # Start first
      async1 = client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "First",
        id: workflow_id,
        task_queue: task_queue,
        execution_timeout: 10.seconds
      )
      
      handle1 = async1.await
      
      # Start duplicate (should fail)
      async2 = client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "Second",
        id: workflow_id,  # Same ID
        task_queue: task_queue,
        execution_timeout: 10.seconds
      )
      
      # Error should be raised on await
      expect_raises(Temporalio::Ext::ExtError, /already running/) do
        async2.await
      end
      
      # First should still work
      result = handle1.result
      result.should eq("Hello, First!")
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
  
  it "pipelines multiple workflow starts efficiently" do
    client = create_client_for_channel
    task_queue = "channel-pipeline-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      num_workflows = 50
      
      # Start all workflows asynchronously (pipelined)
      start_time = Time.monotonic
      async_starts = num_workflows.times.map do |i|
        client.start_workflow_async_channel(
          SimpleCompletionWorkflow.workflow_name,
          "Pipeline#{i}",
          id: "channel-pipeline-#{i}-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 10.seconds
        )
      end.to_a
      pipeline_time = (Time.monotonic - start_time).total_milliseconds
      
      # Should return very fast (not waiting for server responses)
      pipeline_time.should be < 100
      
      # Wait for all to complete
      handles = Temporalio::Client.await_all(async_starts)
      handles.size.should eq(num_workflows)
      
      # Get all results
      results = handles.map(&.result)
      num_workflows.times do |i|
        results[i].should eq("Hello, Pipeline#{i}!")
      end
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
  
  it "supports await_all_settled for mixed success/failure" do
    client = create_client_for_channel
    task_queue = "channel-settled-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      dup_id = "channel-dup-#{Random.rand(10000)}"
      
      async_starts = [
        client.start_workflow_async_channel(
          SimpleCompletionWorkflow.workflow_name,
          "Good1",
          id: "channel-good1-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 10.seconds
        ),
        client.start_workflow_async_channel(
          SimpleCompletionWorkflow.workflow_name,
          "Dup1",
          id: dup_id,
          task_queue: task_queue,
          execution_timeout: 10.seconds
        ),
        client.start_workflow_async_channel(
          SimpleCompletionWorkflow.workflow_name,
          "Good2",
          id: "channel-good2-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 10.seconds
        ),
        client.start_workflow_async_channel(
          SimpleCompletionWorkflow.workflow_name,
          "Dup2",
          id: dup_id,  # Duplicate
          task_queue: task_queue,
          execution_timeout: 10.seconds
        ),
      ]
      
      # Wait for all (won't raise on failures)
      results = Temporalio::Client.await_all_settled(async_starts)
      
      # Count successes and failures
      successes = results.count { |r| r[:success] }
      failures = results.count { |r| !r[:success] }
      
      successes.should eq(3)
      failures.should eq(1)
      
      # Check the failure is correct type
      failed = results.find { |r| !r[:success] }
      failed.should_not be_nil
      failed.not_nil![:error].should be_a(Temporalio::Ext::ExtError)
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
  
  it "completes quickly demonstrating pipelining benefit" do
    client = create_client_for_channel
    task_queue = "channel-fast-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleCompletionWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      # Start workflow
      async_start = client.start_workflow_async_channel(
        SimpleCompletionWorkflow.workflow_name,
        "FastTest",
        id: "channel-fast-#{Random.rand(10000)}",
        task_queue: task_queue,
        execution_timeout: 10.seconds
      )
      
      # Should complete reasonably quickly
      start_time = Time.monotonic
      handle = async_start.await
      elapsed = (Time.monotonic - start_time).total_milliseconds
      
      # Verify it worked
      handle.should be_a(Temporalio::Client::WorkflowHandle)
      result = handle.result
      result.should eq("Hello, FastTest!")
      
      # Should complete in under 2 seconds
      elapsed.should be < 2000
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
end
