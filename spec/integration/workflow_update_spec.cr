require "../spec_helper"
require "./workflows/update_workflow"

def unique_update_id(prefix)
  "#{prefix}-#{Time.utc.to_unix_ms}-#{Random.rand(10000)}"
end

UPDATE_CLIENT = Temporalio::Client.connect("http://localhost:7234", namespace: "default")
UPDATE_TASK_QUEUE = "update-integration-#{Time.utc.to_unix_ms}"

UPDATE_WORKER = begin
  workflow_defs = [
    Temporalio::Internal::ConcreteWorkflowDefinition(SimpleUpdateWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ValidatedUpdateWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(AsyncUpdateWorkflow).new,
  ] of Temporalio::Internal::WorkflowDefinition

  w = Temporalio::Worker.new(
    client: UPDATE_CLIENT,
    task_queue: UPDATE_TASK_QUEUE,
    workflows: workflow_defs,
    activities: [] of Temporalio::Internal::ActivityDefinition
  )
  spawn { w.run }
  sleep 50.milliseconds
  w
end

describe "Workflow Updates Integration Tests" do
  # Force worker initialization
  _ = UPDATE_WORKER

  it "executes update and returns result" do
    with_timeout(60.seconds) do
      handle = UPDATE_CLIENT.start_workflow(
        SimpleUpdateWorkflow.workflow_name,
        id: unique_update_id("simple-update"),
        task_queue: UPDATE_TASK_QUEUE,
        execution_timeout: 30.seconds
      )

      result = handle.execute_update("increment", 5_i64)
      result.should eq("5")

      result = handle.execute_update("increment", 3_i64)
      result.should eq("8")
    ensure
      handle.try(&.terminate) rescue nil
    end
  end

  it "rejects update when validator fails" do
    with_timeout(60.seconds) do
      handle = UPDATE_CLIENT.start_workflow(
        ValidatedUpdateWorkflow.workflow_name,
        id: unique_update_id("validated-update"),
        task_queue: UPDATE_TASK_QUEUE,
        execution_timeout: 30.seconds
      )

      expect_raises(Exception, /Insufficient funds/) do
        handle.execute_update("withdraw", 200_i64)
      end

      result = handle.execute_update("withdraw", 50_i64)
      result.should eq("50")
    ensure
      handle.try(&.terminate) rescue nil
    end
  end

  it "starts update without waiting for completion" do
    with_timeout(60.seconds) do
      handle = UPDATE_CLIENT.start_workflow(
        SimpleUpdateWorkflow.workflow_name,
        id: unique_update_id("start-update"),
        task_queue: UPDATE_TASK_QUEUE,
        execution_timeout: 30.seconds
      )

      update_handle = handle.start_update("increment", 10_i64)
      update_handle.should be_a(Temporalio::Client::UpdateHandle)

      result = update_handle.result
      result.should eq("10")
    ensure
      handle.try(&.terminate) rescue nil
    end
  end

  it "allows update handlers to use workflow operations" do
    with_timeout(60.seconds) do
      handle = UPDATE_CLIENT.start_workflow(
        AsyncUpdateWorkflow.workflow_name,
        id: unique_update_id("async-update"),
        task_queue: UPDATE_TASK_QUEUE,
        execution_timeout: 30.seconds
      )

      result = handle.execute_update("add_message", "Hello")
      result.should eq("1")

      result = handle.execute_update("add_message", "World")
      result.should eq("2")
    ensure
      handle.try(&.terminate) rescue nil
    end
  end
end
