require "../spec_helper"
require "../../src/temporalio/testing/workflow_environment"
require "./workflows/simple_completion_workflow"
require "./workflows/activity_retry_workflow"
require "./workflows/timer_workflow"
require "./workflows/signal_accumulator_workflow"
require "./workflows/cancellation_workflow"
require "./workflows/child_workflow_parent"
require "./workflows/continue_as_new_workflow"
require "./workflows/worker_crash_workflow"
require "./workflows/determinism_test_workflow"
require "./workflows/parallel_activities_workflow"
require "./workflows/activity_timeout_workflow"
require "./workflows/infinite_loop_workflow"
require "./workflows/non_retryable_workflow"
require "./activities/retryable_activity"
require "./activities/deterministic_activity"
require "./activities/parallel_activity"
require "./activities/slow_activity"
require "./activities/non_retryable_activity"

def unique_id(prefix)
  "#{prefix}-#{Time.utc.to_unix_ms}-#{Random.rand(10000)}"
end

# Shared client and worker for all tests - avoids repeated connect/setup overhead
INTEGRATION_CLIENT = Temporalio::Client.connect("http://localhost:7234", namespace: "default")
INTEGRATION_TASK_QUEUE = "integration-#{Time.utc.to_unix_ms}"

INTEGRATION_WORKER = begin
  workflow_defs = [
    Temporalio::Internal::ConcreteWorkflowDefinition(SimpleCompletionWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ActivityRetryWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(TimerWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(SignalAccumulatorWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(CancellationWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ChildWorkflowParent).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ChildWorkflowChild).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ContinueAsNewWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(DeterminismTestWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ParallelActivitiesWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(ActivityTimeoutWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(InfiniteLoopWorkflow).new,
    Temporalio::Internal::ConcreteWorkflowDefinition(NonRetryableWorkflow).new,
  ] of Temporalio::Internal::WorkflowDefinition

  activity_defs = [
    Temporalio::Internal::ConcreteActivityDefinition(RetryableActivity).new,
    Temporalio::Internal::ConcreteActivityDefinition(DeterministicActivity).new,
    Temporalio::Internal::ConcreteActivityDefinition(ParallelActivity).new,
    Temporalio::Internal::ConcreteActivityDefinition(SlowActivity).new,
    Temporalio::Internal::ConcreteActivityDefinition(NonRetryableActivity).new,
  ] of Temporalio::Internal::ActivityDefinition

  w = Temporalio::Worker.new(
    client: INTEGRATION_CLIENT,
    task_queue: INTEGRATION_TASK_QUEUE,
    workflows: workflow_defs,
    activities: activity_defs
  )
  spawn { w.run }
  sleep 500.milliseconds
  w
end

def create_crash_worker(client, task_queue)
  workflow_defs = [
    Temporalio::Internal::ConcreteWorkflowDefinition(WorkerCrashWorkflow).new,
  ] of Temporalio::Internal::WorkflowDefinition
  w = Temporalio::Worker.new(
    client: client,
    task_queue: task_queue,
    workflows: workflow_defs,
    activities: [] of Temporalio::Internal::ActivityDefinition
  )
  spawn { w.run }
  sleep 50.milliseconds
  w
end

describe "Temporal Integration Tests (Real Server)" do
  # Force worker initialization
  _ = INTEGRATION_WORKER

  describe "Basic Workflow Execution" do
    it "executes simple workflow to completion" do
      with_timeout(30.seconds) do
        result = INTEGRATION_CLIENT.execute_workflow(
          SimpleCompletionWorkflow.workflow_name,
          "World",
          id: unique_id("simple"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        result.as(String).should eq("Hello, World!")
      end
    end

    it "handles workflow with timers" do
      with_timeout(30.seconds) do
        start_time = Time.utc
        result = INTEGRATION_CLIENT.execute_workflow(
          TimerWorkflow.workflow_name,
          10_i64,
          id: unique_id("timer"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        elapsed = (Time.utc - start_time).total_milliseconds
        result.as(String).should contain("Slept for approximately")
        elapsed.should be >= 10
      end
    end
  end

  describe "Signals and Queries" do
    it "handles signals and queries correctly" do
      with_timeout(60.seconds) do
        handle = INTEGRATION_CLIENT.start_workflow(
          SignalAccumulatorWorkflow.workflow_name,
          id: unique_id("signal"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        handle.signal("add_value", "first")
        handle.signal("add_value", "second")
        handle.signal("add_value", "third")
        handle.signal("finish")
        result = handle.result
        result.should_not be_nil
      end
    end
  end

  describe "Activity Execution and Retries" do
    it "retries failed activities until success" do
      with_timeout(60.seconds) do
        result = INTEGRATION_CLIENT.execute_workflow(
          ActivityRetryWorkflow.workflow_name,
          3_i64,
          id: unique_id("retry"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("Succeeded after retries")
        result.as(String).should contain("attempt 3")
      end
    end

    it "executes multiple activities sequentially" do
      with_timeout(60.seconds) do
        result = INTEGRATION_CLIENT.execute_workflow(
          ParallelActivitiesWorkflow.workflow_name,
          5_i64,
          id: unique_id("multi-activity"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        parsed = JSON.parse(result.as(String))
        parsed.as_a.map(&.as_i64).should eq([0, 10, 20, 30, 40])
      end
    end
  end

  describe "Cancellation" do
    it "handles workflow cancellation gracefully" do
      with_timeout(60.seconds) do
        handle = INTEGRATION_CLIENT.start_workflow(
          CancellationWorkflow.workflow_name,
          id: unique_id("cancel"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        handle.cancel
        result = handle.result
        result.as(String).should eq("Cancelled as expected")
      end
    end
  end

  describe "Child Workflows" do
    it "executes child workflows successfully" do
      with_timeout(60.seconds) do
        result = INTEGRATION_CLIENT.execute_workflow(
          ChildWorkflowParent.workflow_name,
          "TestChild",
          id: unique_id("parent"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        result.as(String).should eq("Parent received: Child says hello to TestChild")
      end
    end
  end

  describe "Continue-As-New" do
    it "handles continue-as-new correctly" do
      with_timeout(60.seconds) do
        handle = INTEGRATION_CLIENT.start_workflow(
          ContinueAsNewWorkflow.workflow_name,
          0_i64,
          5_i64,
          id: unique_id("can"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        result = handle.result(follow_runs: true)
        result.as(String).should eq("5")
      end
    end
  end

  describe "Worker Crash Recovery (Determinism)" do
    it "resumes workflow correctly after worker restart" do
      with_timeout(60.seconds) do
        crash_queue = "test-crash-#{Random.rand(10000)}"
        workflow_id = unique_id("crash")

        worker1 = create_crash_worker(INTEGRATION_CLIENT, crash_queue)

        begin
          handle = INTEGRATION_CLIENT.start_workflow(
            WorkerCrashWorkflow.workflow_name,
            id: workflow_id,
            task_queue: crash_queue,
            execution_timeout: 60.seconds
          )

          sleep 200.milliseconds

          worker1.initiate_shutdown
          worker1.wait_all_complete

          worker2 = create_crash_worker(INTEGRATION_CLIENT, crash_queue)

          begin
            handle.signal("restart")
            result = handle.result
            result.as(String).should eq("5")
          ensure
            worker2.initiate_shutdown
            worker2.wait_all_complete
          end
        rescue ex
          begin
            worker1.initiate_shutdown
            worker1.wait_all_complete
          rescue
          end
          raise ex
        end
      end
    end

    it "maintains determinism across multiple replays" do
      with_timeout(60.seconds) do
        result = INTEGRATION_CLIENT.execute_workflow(
          DeterminismTestWorkflow.workflow_name,
          3_i64,
          id: unique_id("determinism"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("Executions: 3")
        result.as(String).should contain("timer-0,activity-0,timer-1,activity-2,timer-2,activity-4")
      end
    end
  end

  describe "Stress and Load Testing" do
    it "handles 50 concurrent workflows successfully" do
      with_timeout(60.seconds) do
        results = Channel(String).new(50)

        50.times do |i|
          spawn do
            result = INTEGRATION_CLIENT.execute_workflow(
              SimpleCompletionWorkflow.workflow_name,
              "User#{i}",
              id: unique_id("concurrent-#{i}"),
              task_queue: INTEGRATION_TASK_QUEUE,
              execution_timeout: 30.seconds
            )
            results.send(result.as(String))
          end
        end

        collected = [] of String
        50.times { collected << results.receive }

        collected.size.should eq(50)
        expected = 50.times.map { |i| "Hello, User#{i}!" }.to_a
        collected.sort.should eq(expected.sort)
      end
    end

    it "handles rapid signal delivery (100 signals)" do
      with_timeout(60.seconds) do
        handle = INTEGRATION_CLIENT.start_workflow(
          SignalAccumulatorWorkflow.workflow_name,
          id: unique_id("rapid-signal"),
          task_queue: INTEGRATION_TASK_QUEUE,
          execution_timeout: 60.seconds
        )

        100.times do |i|
          handle.signal("add_value", "value-#{i}")
        end

        handle.signal("finish")
        result_str = handle.result
        result_str.should_not be_nil
      end
    end
  end

  describe "Fault Tolerance" do
    it "handles activity timeouts correctly" do
      with_timeout(60.seconds) do
        expect_raises(Temporalio::ActivityError, /timed out/) do
          INTEGRATION_CLIENT.execute_workflow(
            ActivityTimeoutWorkflow.workflow_name,
            id: unique_id("timeout"),
            task_queue: INTEGRATION_TASK_QUEUE,
            execution_timeout: 30.seconds
          )
        end
      end
    end

    it "handles workflow execution timeout" do
      with_timeout(60.seconds) do
        expect_raises(Temporalio::TimeoutError) do
          INTEGRATION_CLIENT.execute_workflow(
            InfiniteLoopWorkflow.workflow_name,
            id: unique_id("wf-timeout"),
            task_queue: INTEGRATION_TASK_QUEUE,
            execution_timeout: 1.second
          )
        end
      end
    end

    it "handles non-retryable activity failures" do
      with_timeout(60.seconds) do
        expect_raises(Temporalio::ActivityError, /failed/) do
          INTEGRATION_CLIENT.execute_workflow(
            NonRetryableWorkflow.workflow_name,
            id: unique_id("nonretry"),
            task_queue: INTEGRATION_TASK_QUEUE,
            execution_timeout: 30.seconds
          )
        end
      end
    end
  end
end
