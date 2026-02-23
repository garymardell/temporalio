require "../spec_helper"
require "./workflows/update_workflow"

# Helper methods for integration tests
def create_client
  Temporalio::Client.connect(
    "http://localhost:7234",
    namespace: "default"
  )
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
    activities: %activity_defs
  )
  
  spawn { %worker.run }
  sleep 100.milliseconds # Give worker time to start
  %worker
end

def unique_id(prefix)
  "#{prefix}-#{Time.utc.to_unix_ms}-#{Random.rand(10000)}"
end

describe "Workflow Updates Integration Tests" do
  it "executes update and returns result" do
    client = create_client
    task_queue = "test-update-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleUpdateWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      handle = client.start_workflow(
        SimpleUpdateWorkflow.workflow_name,
        id: unique_id("simple-update"),
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
      
      # Give workflow time to start
      sleep 500.milliseconds
      
      # Execute first update
      result = handle.execute_update("increment", 5_i64)
      result.should eq("5")
      
      # Execute second update
      result = handle.execute_update("increment", 3_i64)
      result.should eq("8")
    ensure
      handle.try(&.terminate) rescue nil
      worker.try(&.initiate_shutdown)
    end
  end

  it "rejects update when validator fails" do
    client = create_client
    task_queue = "test-validated-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [ValidatedUpdateWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      handle = client.start_workflow(
        ValidatedUpdateWorkflow.workflow_name,
        id: unique_id("validated-update"),
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
      
      # Give workflow time to start
      sleep 500.milliseconds
      
      # Try to withdraw more than balance - should fail validation
      expect_raises(Exception, /Insufficient funds/) do
        handle.execute_update("withdraw", 200_i64)
      end
      
      # Valid withdrawal should succeed
      result = handle.execute_update("withdraw", 50_i64)
      result.should eq("50")
    ensure
      handle.try(&.terminate) rescue nil
      worker.try(&.initiate_shutdown)
    end
  end

  it "starts update without waiting for completion" do
    client = create_client
    task_queue = "test-start-update-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [SimpleUpdateWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      handle = client.start_workflow(
        SimpleUpdateWorkflow.workflow_name,
        id: unique_id("start-update"),
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
      
      # Give workflow time to start
      sleep 500.milliseconds
      
      # Start update without waiting
      update_handle = handle.start_update("increment", 10_i64)
      update_handle.should be_a(Temporalio::Client::UpdateHandle)
      
      # Now wait for result
      result = update_handle.result
      result.should eq("10")
    ensure
      handle.try(&.terminate) rescue nil
      worker.try(&.initiate_shutdown)
    end
  end

  it "allows update handlers to use workflow operations" do
    client = create_client
    task_queue = "test-async-update-#{Random.rand(10000)}"
    
    worker = create_worker(
      client,
      task_queue,
      [AsyncUpdateWorkflow],
      [] of Temporalio::Activity
    )
    
    begin
      handle = client.start_workflow(
        AsyncUpdateWorkflow.workflow_name,
        id: unique_id("async-update"),
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
      
      # Give workflow time to start
      sleep 500.milliseconds
      
      # Execute update that uses ctx.sleep internally
      result = handle.execute_update("add_message", "Hello")
      result.should eq("1")
      
      result = handle.execute_update("add_message", "World")
      result.should eq("2")
    ensure
      handle.try(&.terminate) rescue nil
      worker.try(&.initiate_shutdown)
    end
  end
end
