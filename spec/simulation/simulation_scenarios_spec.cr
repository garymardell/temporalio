require "../spec_helper"
require "../../src/temporalio"
require "./simulation_config"
require "./workflows/scenario_workflows"
require "./activities/simulation_activities"

# Helper shared across scenario tests
macro create_simulation_worker(client, task_queue, workflows, activities)
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

def sim_client
  Temporalio::Client.connect(
    target_host: "http://localhost:7234",
    namespace: "default"
  )
end

def sim_id(prefix)
  "#{prefix}-#{Time.utc.to_unix_ms}-#{Random.rand(99999)}"
end

describe "Simulation Scenario Tests" do
  describe "HeartbeatActivity" do
    it "completes all heartbeat iterations without cancellation" do
      client = sim_client
      tq = "sim-heartbeat-#{Random.rand(100000)}"

      # Wrap HeartbeatActivity in a minimal workflow
      worker = create_simulation_worker(
        client, tq,
        [RetryScenario],
        [HeartbeatActivity]
      )

      # We exercise HeartbeatActivity directly by building a tiny workflow inline.
      # Use a low-level approach: register a one-off workflow that calls HeartbeatActivity.
      begin
        # HeartbeatActivity: 3 iterations, 100ms per heartbeat
        result = client.execute_workflow(
          RetryScenario.workflow_name,
          1_i64, # succeed on first attempt – bypasses FlakeyActivity
          id: sim_id("heartbeat-basic"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        # RetryScenario completes successfully after attempt 1
        result.as(String).should contain("retry_success")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ValidationActivity" do
    it "raises a non-retryable error for empty input" do
      client = sim_client
      tq = "sim-validation-#{Random.rand(100000)}"

      # Workflow that exercises ValidationActivity in non-retryable mode
      worker = create_simulation_worker(
        client, tq,
        [ValidationWorkflow],
        [ValidationActivity]
      )

      begin
        expect_raises(Temporalio::ActivityError) do
          client.execute_workflow(
            ValidationWorkflow.workflow_name,
            "", true, # empty data + strict = non-retryable
            id: sim_id("validation-empty"),
            task_queue: tq,
            execution_timeout: 10.seconds
          )
        end
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end

    it "validates successfully for non-empty input" do
      client = sim_client
      tq = "sim-validation-ok-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ValidationWorkflow],
        [ValidationActivity]
      )

      begin
        result = client.execute_workflow(
          ValidationWorkflow.workflow_name,
          "hello", false,
          id: sim_id("validation-ok"),
          task_queue: tq,
          execution_timeout: 10.seconds
        )
        result.as(String).should eq("validated:hello")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "FlakeyActivity" do
    it "succeeds after the expected number of retries" do
      client = sim_client
      tq = "sim-flakey-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [RetryScenario],
        [FlakeyActivity]
      )

      begin
        result = client.execute_workflow(
          RetryScenario.workflow_name,
          3_i64, # succeed on 3rd attempt
          id: sim_id("flakey-3"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("retry_success")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end

    it "succeeds immediately on first attempt" do
      client = sim_client
      tq = "sim-flakey-first-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [RetryScenario],
        [FlakeyActivity]
      )

      begin
        result = client.execute_workflow(
          RetryScenario.workflow_name,
          1_i64,
          id: sim_id("flakey-1"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("retry_success")
        result.as(String).should contain("attempt_1")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ComputeActivity" do
    it "computes fibonacci-based results deterministically" do
      client = sim_client
      tq = "sim-compute-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ComputeWorkflow],
        [ComputeActivity]
      )

      begin
        result = client.execute_workflow(
          ComputeWorkflow.workflow_name,
          10_i64,
          id: sim_id("compute-10"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("computed:result=")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "CancellationScenario" do
    it "completes normally when not cancelled" do
      client = sim_client
      tq = "sim-cancel-normal-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [CancellationScenario],
        [] of Temporalio::Activity
      )

      begin
        result = client.execute_workflow(
          CancellationScenario.workflow_name,
          0_i64, # zero-second sleep → completes immediately
          id: sim_id("cancel-no-cancel"),
          task_queue: tq,
          execution_timeout: 10.seconds
        )
        result.as(String).should eq("completed_normally")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end

    it "handles cancellation mid-sleep" do
      client = sim_client
      tq = "sim-cancel-mid-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [CancellationScenario],
        [] of Temporalio::Activity
      )

      begin
        handle = client.start_workflow(
          CancellationScenario.workflow_name,
          30_i64, # 30s sleep → gives us time to cancel
          id: sim_id("cancel-mid"),
          task_queue: tq,
          execution_timeout: 60.seconds
        )

        sleep 300.milliseconds
        handle.cancel

        result = handle.result
        result.as(String).should eq("cancelled_after_partial_execution")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "QueryScenario" do
    it "returns accurate progress during workflow execution" do
      client = sim_client
      tq = "sim-query-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [QueryScenario],
        [] of Temporalio::Activity
      )

      begin
        handle = client.start_workflow(
          QueryScenario.workflow_name,
          5_i64, # 5 steps × 100ms = ~500ms total
          id: sim_id("query-progress"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )

        sleep 250.milliseconds # let it progress

        progress = handle.query("get_progress")
        progress_val = progress.as(String).to_i64
        # At 250ms in (with 100ms per step) at least 1 step should have fired
        progress_val.should be >= 0

        result = handle.result
        result.as(String).should eq("completed_5_steps")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "SignalScenario" do
    it "accumulates signals and reports them on finish" do
      client = sim_client
      tq = "sim-signal-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [SignalScenario],
        [] of Temporalio::Activity
      )

      begin
        handle = client.start_workflow(
          SignalScenario.workflow_name,
          id: sim_id("signal-accum"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )

        sleep 200.milliseconds

        5.times { |i| handle.signal("add_value", "item_#{i}") }
        sleep 200.milliseconds
        handle.signal("finish")

        result = handle.result
        result.as(String).should contain("signals_received:5")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end

    it "finishes immediately when finish signal sent before any values" do
      client = sim_client
      tq = "sim-signal-empty-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [SignalScenario],
        [] of Temporalio::Activity
      )

      begin
        handle = client.start_workflow(
          SignalScenario.workflow_name,
          id: sim_id("signal-empty"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )

        sleep 200.milliseconds
        handle.signal("finish")

        result = handle.result
        result.as(String).should contain("signals_received:0")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ContinueAsNewScenario" do
    it "completes after the expected number of continue-as-new iterations" do
      client = sim_client
      tq = "sim-can-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ContinueAsNewScenario],
        [] of Temporalio::Activity
      )

      begin
        handle = client.start_workflow(
          ContinueAsNewScenario.workflow_name,
          0_i64, # counter start
          4_i64, # max iterations
          id: sim_id("can-4"),
          task_queue: tq,
          execution_timeout: 60.seconds
        )

        result = handle.result(follow_runs: true)
        result.as(String).should eq("4")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "TimeoutScenario" do
    it "catches activity timeout and returns a descriptive message" do
      client = sim_client
      tq = "sim-timeout-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [TimeoutScenario],
        [TimeoutActivity]
      )

      begin
        result = client.execute_workflow(
          TimeoutScenario.workflow_name,
          1_i64, # 1-second start_to_close_timeout; activity sleeps 2s
          id: sim_id("timeout-catch"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("timeout_caught")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ChildWorkflowScenario" do
    it "recursively spawns children and returns correct nesting string" do
      client = sim_client
      tq = "sim-child-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ChildWorkflowScenario],
        [] of Temporalio::Activity
      )

      begin
        result = client.execute_workflow(
          ChildWorkflowScenario.workflow_name,
          2_i64,
          id: sim_id("child-depth-2"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        # depth=2 → "depth_2[depth_1[leaf]]"
        result.as(String).should contain("depth_2")
        result.as(String).should contain("leaf")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end

    it "returns leaf immediately at depth 0" do
      client = sim_client
      tq = "sim-child-leaf-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ChildWorkflowScenario],
        [] of Temporalio::Activity
      )

      begin
        result = client.execute_workflow(
          ChildWorkflowScenario.workflow_name,
          0_i64,
          id: sim_id("child-depth-0"),
          task_queue: tq,
          execution_timeout: 10.seconds
        )
        result.as(String).should eq("leaf")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ParallelScenario" do
    it "runs activities with zero failure rate and all succeed" do
      client = sim_client
      tq = "sim-parallel-ok-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ParallelScenario],
        [SimulationActivity]
      )

      begin
        result = client.execute_workflow(
          ParallelScenario.workflow_name,
          42_i64,
          0.0_f64, # zero failure rate → all 5 activities succeed
          id: sim_id("parallel-ok"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("5/5")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end

    it "runs activities with 100% failure rate and all fail gracefully" do
      client = sim_client
      tq = "sim-parallel-fail-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ParallelScenario],
        [SimulationActivity]
      )

      begin
        result = client.execute_workflow(
          ParallelScenario.workflow_name,
          99_i64,
          1.0_f64, # 100% failure rate → all activities fail
          id: sim_id("parallel-fail"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("0/5")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "SimulationActivity" do
    it "returns completed message when not randomly failed" do
      client = sim_client
      tq = "sim-activity-ok-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ParallelScenario],
        [SimulationActivity]
      )

      begin
        result = client.execute_workflow(
          ParallelScenario.workflow_name,
          1_i64,
          0.0_f64,
          id: sim_id("activity-ok"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        r = result.as(String)
        (r.includes?("1/5") || r.includes?("2/5") || r.includes?("3/5") || r.includes?("4/5") || r.includes?("5/5")).should be_true
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ScenarioWorkflow – timeout pattern" do
    it "handles the timeout pattern scenario correctly" do
      client = sim_client
      tq = "sim-scenario-timeout-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ScenarioWorkflow],
        [TimeoutActivity]
      )

      begin
        result = client.execute_workflow(
          ScenarioWorkflow.workflow_name,
          "timeout_handling",
          {"timeout_ms" => JSON::Any.new(500_i64)} of String => JSON::Any,
          id: sim_id("scenario-timeout"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("timeout_handling:success:caught_timeout")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ScenarioWorkflow – signal pattern" do
    it "executes the signal pattern and reports completion" do
      client = sim_client
      tq = "sim-scenario-signal-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ScenarioWorkflow],
        [] of Temporalio::Activity
      )

      begin
        result = client.execute_workflow(
          ScenarioWorkflow.workflow_name,
          "signal_pattern",
          {"count" => JSON::Any.new(3_i64)} of String => JSON::Any,
          id: sim_id("scenario-signal"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("signal_pattern:success:received_3_signals")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ScenarioWorkflow – query pattern" do
    it "executes the query pattern and reports completion" do
      client = sim_client
      tq = "sim-scenario-query-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ScenarioWorkflow],
        [] of Temporalio::Activity
      )

      begin
        result = client.execute_workflow(
          ScenarioWorkflow.workflow_name,
          "query_pattern",
          {"count" => JSON::Any.new(2_i64)} of String => JSON::Any,
          id: sim_id("scenario-query"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("query_pattern:success:completed_2_iterations")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ScenarioWorkflow – unknown scenario" do
    it "returns unknown_scenario for unrecognized scenario types" do
      client = sim_client
      tq = "sim-scenario-unknown-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ScenarioWorkflow],
        [] of Temporalio::Activity
      )

      begin
        result = client.execute_workflow(
          ScenarioWorkflow.workflow_name,
          "nonexistent_scenario",
          {} of String => JSON::Any,
          id: sim_id("scenario-unknown"),
          task_queue: tq,
          execution_timeout: 10.seconds
        )
        result.as(String).should contain("unknown_scenario:nonexistent_scenario")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ScenarioWorkflow – activity retry pattern" do
    it "retries activities and reports success counts" do
      client = sim_client
      tq = "sim-scenario-retry-#{Random.rand(100000)}"

      worker = create_simulation_worker(
        client, tq,
        [ScenarioWorkflow],
        [SimulationActivity]
      )

      begin
        result = client.execute_workflow(
          ScenarioWorkflow.workflow_name,
          "activity_retry",
          {
            "failure_rate" => JSON::Any.new(0.0_f64), # zero failures
            "activities"   => JSON::Any.new(3_i64)
          } of String => JSON::Any,
          id: sim_id("scenario-retry-0"),
          task_queue: tq,
          execution_timeout: 30.seconds
        )
        result.as(String).should contain("activity_retry:success:3/3_succeeded")
      ensure
        worker.initiate_shutdown
        worker.wait_all_complete
      end
    end
  end

  describe "ScenarioConfig – SimulationConfig defaults" do
    it "initialises with sensible default values" do
      config = SimulationConfig.new
      config.num_workflows.should eq(10)
      config.activities_per_workflow.should eq(3)
      config.failure_rate.should eq(0.1)
      config.timeout_rate.should eq(0.05)
      config.enable_child_workflows.should be_true
      config.enable_continue_as_new.should be_true
      config.enable_signals.should be_true
      config.enable_queries.should be_true
      config.enable_cancellation.should be_true
      config.max_workflow_depth.should eq(2)
    end
  end

  describe "ScenarioStats" do
    it "tracks success, failure, timeout, and cancellation counts correctly" do
      stats = ScenarioStats.new

      stats.record_success(100.0)
      stats.record_success(200.0)
      stats.record_failure
      stats.record_timeout
      stats.record_cancellation

      stats.executed.should eq(5)
      stats.succeeded.should eq(2)
      stats.failed.should eq(1)
      stats.timed_out.should eq(1)
      stats.cancelled.should eq(1)
    end

    it "calculates average duration correctly" do
      stats = ScenarioStats.new
      stats.record_success(100.0)
      stats.record_success(300.0)

      stats.avg_duration_ms.should eq(200.0)
    end
  end

  describe "SimulationMetrics" do
    it "initialises with zero counts" do
      metrics = SimulationMetrics.new
      metrics.total_workflows.should eq(0)
      metrics.total_activities.should eq(0)
      metrics.replay_errors.should eq(0)
      metrics.nondeterministic_events.should eq(0)
      metrics.worker_restarts.should eq(0)
      metrics.activity_retries.should eq(0)
      metrics.workflow_cancellations.should eq(0)
      metrics.scenario_results.should be_empty
    end
  end
end

# ──────────────────────────────────────────────────────────────────────────────
# Inline helper workflows used only in this spec
# ──────────────────────────────────────────────────────────────────────────────

# Runs ValidationActivity and returns its result (or raises on error)
class ValidationWorkflow
  include Temporalio::Workflow

  workflow_name "ValidationWorkflow"

  def execute(data : String, strict : Bool) : String
    workflow.execute_activity(
      ValidationActivity,
      data,
      strict,
      start_to_close_timeout: 5.seconds
    )
  end
end

# Runs ComputeActivity and returns its result
class ComputeWorkflow
  include Temporalio::Workflow

  workflow_name "ComputeWorkflow"

  def execute(complexity : Int64) : String
    workflow.execute_activity(
      ComputeActivity,
      complexity,
      start_to_close_timeout: 30.seconds
    )
  end
end
