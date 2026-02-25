require "../spec_helper"
require "../support/activities/hello_activity"
require "../support/activities/failing_activity"
require "../support/activities/heartbeat_activity"
require "../support/workflows/simple_workflow"
require "../support/workflows/timer_workflow"
require "../support/workflows/signal_workflow"
require "../support/workflows/query_workflow"
require "../support/workflows/activity_workflow"
require "../support/workflows/cancellation_workflow"
require "../support/workflows/child_workflow"
require "../support/workflows/continue_as_new_workflow"
require "../support/workflows/patch_workflow"

# ─── Helpers ────────────────────────────────────────────────────────────────

# Build a fresh activation with just an InitializeWorkflow job.
private def init_activation(
  run_id : String,
  workflow_type : String,
  args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
  workflow_id : String = "wf-id",
  attempt : Int32 = 1,
  timestamp_secs : Int64 = Time.utc.to_unix
) : Coresdk::WorkflowActivation::WorkflowActivation
  Coresdk::WorkflowActivation::WorkflowActivation.new(
    run_id: run_id,
    timestamp: Google::Protobuf::Timestamp.new(seconds: timestamp_secs),
    jobs: [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
          workflow_type: workflow_type,
          workflow_id: workflow_id,
          attempt: attempt,
          arguments: args
        )
      ),
    ]
  )
end

# Build a follow-up activation with given jobs.
private def followup_activation(
  run_id : String,
  jobs : Array(Coresdk::WorkflowActivation::WorkflowActivationJob),
  timestamp_secs : Int64 = Time.utc.to_unix
) : Coresdk::WorkflowActivation::WorkflowActivation
  Coresdk::WorkflowActivation::WorkflowActivation.new(
    run_id: run_id,
    timestamp: Google::Protobuf::Timestamp.new(seconds: timestamp_secs),
    jobs: jobs
  )
end

private def dc
  Temporalio::DataConverter::DEFAULT
end

# ─── Tests ──────────────────────────────────────────────────────────────────

describe "Temporalio Workflow Engine" do
  # ── Basic execution ────────────────────────────────────────────────────────

  describe "simple workflow" do
    it "completes with correct return value" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SimpleWorkflow", [dc.to_payload("Crystal")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(SimpleWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_true
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.size.should eq(1)
      result = dc.from_payload(cmds[0].complete_workflow_execution.not_nil!.result.not_nil!, String)
      result.should eq("Hello, Crystal!")
    end

    it "workflow_name defaults to class name" do
      SimpleWorkflow.workflow_name.should eq("SimpleWorkflow")
    end

    it "handles zero-arg execute" do
      # SingleSignalWorkflow has no explicit execute args but needs signal to complete
      # — tested in signal section; here just test workflow_name
      SingleSignalWorkflow.workflow_name.should eq("SingleSignalWorkflow")
    end
  end

  # ── Timers / sleep ─────────────────────────────────────────────────────────

  describe "timers" do
    it "emits StartTimer command and suspends" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "TimerWorkflow", [dc.to_payload(5_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_false
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.size.should eq(1)
      timer_cmd = cmds[0].start_timer.not_nil!
      timer_cmd.seq.should eq(1_u32)
      duration = timer_cmd.start_to_fire_timeout.not_nil!
      duration.seconds.should eq(5_i64)
    end

    it "completes after FireTimer job" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "TimerWorkflow", [dc.to_payload(3_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      # First activation yields a StartTimer
      inst.complete?.should be_false
      seq = comp1.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq

      # Second activation fires the timer
      fire_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq)
        ),
      ])
      comp2 = inst.apply_activation(fire_act)

      inst.complete?.should be_true
      result = dc.from_payload(
        comp2.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("slept 3s")
    end

    it "emits multiple sequential StartTimer commands" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "MultiTimerWorkflow", [dc.to_payload(3_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(MultiTimerWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")

      # Fire all 3 timers one by one
      comp = inst.apply_activation(act)
      3.times do |i|
        inst.complete?.should be_false
        seq = comp.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq
        comp = inst.apply_activation(followup_activation(run_id, [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq)
          ),
        ]))
      end

      inst.complete?.should be_true
      result = dc.from_payload(
        comp.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        Int64
      )
      result.should eq(3_i64)
    end

    it "deterministic time advances with activation timestamp" do
      run_id = "run-#{Random.new.hex(8)}"
      t0 = 1_700_000_000_i64
      act = init_activation(run_id, "TimerWorkflow", [dc.to_payload(1_i64)], timestamp_secs: t0)
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)

      t1 = t0 + 1
      fire_act = Coresdk::WorkflowActivation::WorkflowActivation.new(
        run_id: run_id,
        timestamp: Google::Protobuf::Timestamp.new(seconds: t1),
        jobs: [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: 1_u32)
          ),
        ]
      )
      inst.apply_activation(fire_act)
      inst.complete?.should be_true
    end
  end

  # ── Signals ────────────────────────────────────────────────────────────────

  describe "signals" do
    it "workflow_signal macro dispatches a single signal" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SingleSignalWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(SingleSignalWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      # Should be waiting (wait_condition on @received)
      inst.complete?.should be_false
      comp1.successful.not_nil!.commands.not_nil!.should be_empty

      # Send the signal
      sig_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "my-signal",
            input: [dc.to_payload("hello from signal")]
          )
        ),
      ])
      comp2 = inst.apply_activation(sig_act)

      inst.complete?.should be_true
      result = dc.from_payload(
        comp2.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("hello from signal")
    end

    it "workflow_signal macro dispatches multiple distinct signals" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "MultiSignalWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(MultiSignalWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)
      inst.complete?.should be_false

      # Send increment signal
      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "increment",
            input: [dc.to_payload(7_i64)]
          )
        ),
      ]))
      inst.complete?.should be_false

      # Send set-name signal — now both conditions satisfied, workflow completes
      comp3 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "set-name",
            input: [dc.to_payload("crystal")]
          )
        ),
      ]))
      inst.complete?.should be_true
      result = dc.from_payload(
        comp3.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("crystal:7")
    end

    it "unknown signal is silently ignored" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SingleSignalWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(SingleSignalWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)

      # Send unknown signal — should not crash
      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "unknown-signal",
            input: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ]))
      inst.complete?.should be_false # still waiting for "my-signal"
    end

    it "multiple signals in same activation all fire" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "MultiSignalWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(MultiSignalWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)

      # Both signals in one activation
      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "increment",
            input: [dc.to_payload(3_i64)]
          )
        ),
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "set-name",
            input: [dc.to_payload("batch")]
          )
        ),
      ]))

      inst.complete?.should be_true
      result = dc.from_payload(
        comp2.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("batch:3")
    end
  end

  # ── Queries ────────────────────────────────────────────────────────────────

  describe "queries" do
    it "workflow_query macro responds to a single query" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SingleQueryWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(SingleQueryWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act) # starts timer, suspends

      inst.complete?.should be_false

      # Send a query
      query_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          query_workflow: Coresdk::WorkflowActivation::QueryWorkflow.new(
            query_id: "q1",
            query_type: "get-state",
            arguments: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ])
      comp2 = inst.apply_activation(query_act)

      cmds = comp2.successful.not_nil!.commands.not_nil!
      query_cmd = cmds.find(&.respond_to_query)
      query_cmd.should_not be_nil
      resp = query_cmd.not_nil!.respond_to_query.not_nil!
      resp.query_id.should eq("q1")
      result = dc.from_payload(resp.succeeded.not_nil!.response.not_nil!, String)
      result.should eq("initial")
    end

    it "workflow_query macro dispatches multiple distinct queries" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "MultiQueryWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(MultiQueryWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)

      # Query get-count
      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          query_workflow: Coresdk::WorkflowActivation::QueryWorkflow.new(
            query_id: "q-count",
            query_type: "get-count",
            arguments: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ]))
      count_cmd = comp2.successful.not_nil!.commands.not_nil!.find(&.respond_to_query).not_nil!
      dc.from_payload(count_cmd.respond_to_query.not_nil!.succeeded.not_nil!.response.not_nil!, Int64).should eq(42_i64)

      # Query get-label
      comp3 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          query_workflow: Coresdk::WorkflowActivation::QueryWorkflow.new(
            query_id: "q-label",
            query_type: "get-label",
            arguments: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ]))
      label_cmd = comp3.successful.not_nil!.commands.not_nil!.find(&.respond_to_query).not_nil!
      dc.from_payload(label_cmd.respond_to_query.not_nil!.succeeded.not_nil!.response.not_nil!, String).should eq("hello")
    end

    it "query with typed argument" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "QueryWithArgWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(QueryWithArgWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)

      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          query_workflow: Coresdk::WorkflowActivation::QueryWorkflow.new(
            query_id: "q1",
            query_type: "multiply",
            arguments: [dc.to_payload(5_i64)]
          )
        ),
      ]))
      cmd = comp2.successful.not_nil!.commands.not_nil!.find(&.respond_to_query).not_nil!
      # @value is 0 * 5 = 0
      dc.from_payload(cmd.respond_to_query.not_nil!.succeeded.not_nil!.response.not_nil!, Int64).should eq(0_i64)
    end

    it "unknown query responds with nil payload" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SingleQueryWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(SingleQueryWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)

      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          query_workflow: Coresdk::WorkflowActivation::QueryWorkflow.new(
            query_id: "q-unknown",
            query_type: "no-such-query",
            arguments: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ]))
      cmd = comp2.successful.not_nil!.commands.not_nil!.find(&.respond_to_query).not_nil!
      cmd.respond_to_query.not_nil!.query_id.should eq("q-unknown")
      # succeeded with nil response is fine
      cmd.respond_to_query.not_nil!.succeeded.should_not be_nil
    end
  end

  # ── Activities ─────────────────────────────────────────────────────────────

  describe "activity scheduling" do
    it "emits ScheduleActivity command and suspends" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ActivityWorkflow", [dc.to_payload("test")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(ActivityWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_false
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.size.should eq(1)
      sched = cmds[0].schedule_activity.not_nil!
      sched.activity_type.should eq("HelloActivity")
      sched.seq.should eq(1_u32)
    end

    it "completes after ResolveActivity job (success)" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ActivityWorkflow", [dc.to_payload("World")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(ActivityWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      seq = comp1.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq
      result_payload = dc.to_payload("Hello, World!")

      resolve_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_activity: Coresdk::WorkflowActivation::ResolveActivity.new(
            seq: seq,
            result: Coresdk::ActivityResult::ActivityResolution.new(
              completed: Coresdk::ActivityResult::Success.new(result: result_payload)
            )
          )
        ),
      ])
      comp2 = inst.apply_activation(resolve_act)

      inst.complete?.should be_true
      result = dc.from_payload(
        comp2.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("Hello, World!")
    end

    it "fails workflow when activity fails" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "FailingActivityWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(FailingActivityWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      seq = comp1.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq

      resolve_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_activity: Coresdk::WorkflowActivation::ResolveActivity.new(
            seq: seq,
            result: Coresdk::ActivityResult::ActivityResolution.new(
              failed: Coresdk::ActivityResult::Failure.new(
                failure: Temporal::Api::Failure::V1::Failure.new(
                  message: "expected failure",
                  application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
                    type: "TestError",
                    non_retryable: true
                  )
                )
              )
            )
          )
        ),
      ])
      comp2 = inst.apply_activation(resolve_act)

      inst.complete?.should be_true
      # Workflow should fail (the exception propagates up to the workflow fiber)
      cmds = comp2.successful.not_nil!.commands.not_nil!
      cmds.any?(&.fail_workflow_execution).should be_true
    end

    it "activity cancelled raises CancelledError" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ActivityWorkflow", [dc.to_payload("x")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(ActivityWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      seq = comp1.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq

      resolve_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_activity: Coresdk::WorkflowActivation::ResolveActivity.new(
            seq: seq,
            result: Coresdk::ActivityResult::ActivityResolution.new(
              cancelled: Coresdk::ActivityResult::Cancellation.new
            )
          )
        ),
      ])
      comp2 = inst.apply_activation(resolve_act)

      inst.complete?.should be_true
      cmds = comp2.successful.not_nil!.commands.not_nil!
      cmds.any?(&.fail_workflow_execution).should be_true
      failure_msg = cmds.find(&.fail_workflow_execution).not_nil!
        .fail_workflow_execution.not_nil!.failure.not_nil!.message.not_nil!
      failure_msg.should contain("cancelled")
    end

    it "emits ScheduleLocalActivity command" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "LocalActivityWorkflow", [dc.to_payload("local")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(LocalActivityWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_false
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.size.should eq(1)
      cmds[0].schedule_local_activity.should_not be_nil
      cmds[0].schedule_local_activity.not_nil!.activity_type.should eq("HelloActivity")
    end

    it "sequential activities emit commands one at a time" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SequentialActivitiesWorkflow", [dc.to_payload(3_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(SequentialActivitiesWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")

      comp = inst.apply_activation(act)
      total = 0_i64

      3.times do |i|
        inst.complete?.should be_false
        seq = comp.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq
        result_str = "Hello, item#{i}!"
        total += result_str.size.to_i64
        comp = inst.apply_activation(followup_activation(run_id, [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            resolve_activity: Coresdk::WorkflowActivation::ResolveActivity.new(
              seq: seq,
              result: Coresdk::ActivityResult::ActivityResolution.new(
                completed: Coresdk::ActivityResult::Success.new(result: dc.to_payload(result_str))
              )
            )
          ),
        ]))
      end

      inst.complete?.should be_true
      result = dc.from_payload(
        comp.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        Int64
      )
      result.should eq(total)
    end
  end

  # ── Cancellation ───────────────────────────────────────────────────────────

  describe "cancellation" do
    it "CancelWorkflow job sets cancelled flag and check_cancellation! raises" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "CancellationWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitCancellationWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      inst.complete?.should be_false
      # First activation emits StartTimer (sleep 1000s)
      comp1.successful.not_nil!.commands.not_nil![0].start_timer.should_not be_nil
      timer_seq = comp1.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq

      # Cancel + fire timer (timer may have been cancelled but fires anyway in test)
      cancel_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          cancel_workflow: Coresdk::WorkflowActivation::CancelWorkflow.new
        ),
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: timer_seq)
        ),
      ])
      comp2 = inst.apply_activation(cancel_act)

      inst.complete?.should be_true
      cmds = comp2.successful.not_nil!.commands.not_nil!
      cmds.any?(&.fail_workflow_execution).should be_true
      failure_msg = cmds.find(&.fail_workflow_execution).not_nil!
        .fail_workflow_execution.not_nil!.failure.not_nil!.message.not_nil!
      failure_msg.should contain("cancelled")
    end

    it "workflow can handle cancellation and return gracefully" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "CancellationWithCleanupWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(CancellationWithCleanupWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)
      inst.complete?.should be_false

      # Send cancel — wait_condition checks cancelled? so the workflow will finish
      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          cancel_workflow: Coresdk::WorkflowActivation::CancelWorkflow.new
        ),
      ]))

      inst.complete?.should be_true
      cmds = comp2.successful.not_nil!.commands.not_nil!
      # Should complete successfully (not fail) because the rescue caught it
      cmds.any?(&.complete_workflow_execution).should be_true
      result = dc.from_payload(
        cmds.find(&.complete_workflow_execution).not_nil!.complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("cancelled-and-cleaned-up")
    end
  end

  # ── Child workflows ────────────────────────────────────────────────────────

  describe "child workflows" do
    it "emits StartChildWorkflowExecution command and suspends" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ParentWorkflow", [dc.to_payload("test")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(ParentWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_false
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.size.should eq(1)
      child_cmd = cmds[0].start_child_workflow_execution.not_nil!
      child_cmd.workflow_type.should eq("ChildWorkflow")
      child_cmd.seq.should eq(1_u32)
    end

    it "completes after child start + complete resolution" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ParentWorkflow", [dc.to_payload("Alice")])
      wf = Temporalio::Internal::ConcreteWorkflowObject(ParentWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      seq = comp1.successful.not_nil!.commands.not_nil![0].start_child_workflow_execution.not_nil!.seq
      child_run_id = "child-run-#{Random.new.hex(4)}"

      # Phase 1: child started
      comp2 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_child_workflow_execution_start: Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStart.new(
            seq: seq,
            succeeded: Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStartSuccess.new(
              run_id: child_run_id
            )
          )
        ),
      ]))
      inst.complete?.should be_false

      # Phase 2: child completed
      comp3 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_child_workflow_execution: Coresdk::WorkflowActivation::ResolveChildWorkflowExecution.new(
            seq: seq,
            result: Coresdk::ChildWorkflow::ChildWorkflowResult.new(
              completed: Coresdk::ChildWorkflow::Success.new(
                result: dc.to_payload("child:Alice")
              )
            )
          )
        ),
      ]))

      inst.complete?.should be_true
      result = dc.from_payload(
        comp3.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("child:Alice")
    end

    it "child workflow failure propagates as Error" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ParentWithFailingChildWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(ParentWithFailingChildWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(act)

      seq = comp1.successful.not_nil!.commands.not_nil![0].start_child_workflow_execution.not_nil!.seq

      # Start the child
      inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_child_workflow_execution_start: Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStart.new(
            seq: seq,
            succeeded: Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStartSuccess.new(
              run_id: "child-run-fail"
            )
          )
        ),
      ]))

      # Fail the child
      comp3 = inst.apply_activation(followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          resolve_child_workflow_execution: Coresdk::WorkflowActivation::ResolveChildWorkflowExecution.new(
            seq: seq,
            result: Coresdk::ChildWorkflow::ChildWorkflowResult.new(
              failed: Coresdk::ChildWorkflow::Failure.new(
                failure: Temporal::Api::Failure::V1::Failure.new(
                  message: "child failed",
                  application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
                    type: "ChildError",
                    non_retryable: true
                  )
                )
              )
            )
          )
        ),
      ]))

      # ParentWithFailingChildWorkflow rescues the error and returns a string
      inst.complete?.should be_true
      result = dc.from_payload(
        comp3.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should start_with("caught:")
    end
  end

  # ── Continue-as-new ────────────────────────────────────────────────────────

  describe "continue-as-new" do
    it "emits ContinueAsNewWorkflowExecution command" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ContinueAsNewWorkflow", [dc.to_payload(3_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitContinueAsNewWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_true
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.any?(&.continue_as_new_workflow_execution).should be_true
    end

    it "ContinueAsNew passes encoded args" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ContinueAsNewWorkflow", [dc.to_payload(5_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitContinueAsNewWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      can_cmd = comp.successful.not_nil!.commands.not_nil![0].continue_as_new_workflow_execution.not_nil!
      args = can_cmd.arguments.not_nil!
      args.size.should eq(1)
      dc.from_payload(args[0], Int64).should eq(4_i64) # count - 1
    end

    it "workflow terminates immediately when count reaches 0" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "ContinueAsNewWorkflow", [dc.to_payload(0_i64)])
      wf = Temporalio::Internal::ConcreteWorkflowObject(UnitContinueAsNewWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_true
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.any?(&.complete_workflow_execution).should be_true
      result = dc.from_payload(
        cmds.find(&.complete_workflow_execution).not_nil!.complete_workflow_execution.not_nil!.result.not_nil!,
        Int64
      )
      result.should eq(0_i64)
    end
  end

  # ── Patches ────────────────────────────────────────────────────────────────

  describe "patches" do
    it "patched? returns false without NotifyHasPatch job and emits SetPatchMarker" do
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "PatchedBehaviorWorkflow")
      wf = Temporalio::Internal::ConcreteWorkflowObject(PatchedBehaviorWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_true
      cmds = comp.successful.not_nil!.commands.not_nil!
      patch_cmd = cmds.find(&.set_patch_marker)
      patch_cmd.should_not be_nil
      patch_cmd.not_nil!.set_patch_marker.not_nil!.patch_id.should eq("v2-behavior")

      result = dc.from_payload(
        cmds.find(&.complete_workflow_execution).not_nil!.complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("old-behavior")
    end

    it "patched? returns true when NotifyHasPatch job is present" do
      run_id = "run-#{Random.new.hex(8)}"
      init_job = Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
          workflow_type: "PatchedBehaviorWorkflow",
          workflow_id: "wf-id",
          attempt: 1,
          arguments: [] of Temporal::Api::Common::V1::Payload
        )
      )
      patch_job = Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        notify_has_patch: Coresdk::WorkflowActivation::NotifyHasPatch.new(patch_id: "v2-behavior")
      )
      act = Coresdk::WorkflowActivation::WorkflowActivation.new(
        run_id: run_id,
        timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
        jobs: [init_job, patch_job]
      )

      wf = Temporalio::Internal::ConcreteWorkflowObject(PatchedBehaviorWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_true
      cmds = comp.successful.not_nil!.commands.not_nil!
      result = dc.from_payload(
        cmds.find(&.complete_workflow_execution).not_nil!.complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("new-behavior")
    end

    it "multiple patches are independently tracked" do
      run_id = "run-#{Random.new.hex(8)}"
      init_job = Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
          workflow_type: "MultiPatchWorkflow",
          workflow_id: "wf-id",
          attempt: 1,
          arguments: [] of Temporal::Api::Common::V1::Payload
        )
      )
      patch_v2 = Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        notify_has_patch: Coresdk::WorkflowActivation::NotifyHasPatch.new(patch_id: "patch-v2")
      )
      act = Coresdk::WorkflowActivation::WorkflowActivation.new(
        run_id: run_id,
        timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
        jobs: [init_job, patch_v2]
      )

      wf = Temporalio::Internal::ConcreteWorkflowObject(MultiPatchWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(act)

      inst.complete?.should be_true
      result = dc.from_payload(
        comp.successful.not_nil!.commands.not_nil!
          .find(&.complete_workflow_execution).not_nil!
          .complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      # Only patch-v2 was notified; patch-v3 was not
      result.should eq("v2")
    end
  end

  # ── WorkflowRunner ─────────────────────────────────────────────────────────

  describe "WorkflowRunner" do
    it "routes activations by workflow type" do
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(SimpleWorkflow).new)

      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "SimpleWorkflow", [dc.to_payload("Runner")])
      bytes = runner.handle_activation(act.to_protobuf.to_slice)
      comp = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes))

      runner.cached_size.should eq(0)
      result = dc.from_payload(
        comp.successful.not_nil!.commands.not_nil!
          .find(&.complete_workflow_execution).not_nil!
          .complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("Hello, Runner!")
    end

    it "caches in-progress workflows across multiple activations" do
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(UnitTimerWorkflow).new)

      run_id = "run-#{Random.new.hex(8)}"
      act1 = init_activation(run_id, "TimerWorkflow", [dc.to_payload(1_i64)])
      bytes1 = runner.handle_activation(act1.to_protobuf.to_slice)
      comp1 = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes1))

      runner.cached_size.should eq(1)
      seq = comp1.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq

      act2 = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq)
        ),
      ])
      bytes2 = runner.handle_activation(act2.to_protobuf.to_slice)
      comp2 = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes2))

      runner.cached_size.should eq(0)
      comp2.successful.not_nil!.commands.not_nil!.any?(&.complete_workflow_execution).should be_true
    end

    it "returns failure for unknown workflow type" do
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      run_id = "run-#{Random.new.hex(8)}"
      act = init_activation(run_id, "NoSuchWorkflow")
      bytes = runner.handle_activation(act.to_protobuf.to_slice)
      comp = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes))

      comp.failed.should_not be_nil
      comp.failed.not_nil!.failure.not_nil!.message.not_nil!.should contain("NoSuchWorkflow")
    end

    it "removes workflow from cache on remove_from_cache job" do
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(UnitTimerWorkflow).new)

      run_id = "run-#{Random.new.hex(8)}"
      act1 = init_activation(run_id, "TimerWorkflow", [dc.to_payload(1_i64)])
      runner.handle_activation(act1.to_protobuf.to_slice)
      runner.cached_size.should eq(1)

      evict_act = followup_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          remove_from_cache: Coresdk::WorkflowActivation::RemoveFromCache.new(
            message: "evicted",
            reason: Coresdk::WorkflowActivation::RemoveFromCache::EvictionReason::UNSPECIFIED.value
          )
        ),
      ])
      runner.handle_activation(evict_act.to_protobuf.to_slice)
      runner.cached_size.should eq(0)
    end

    it "handles concurrent independent workflow runs" do
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(SimpleWorkflow).new)

      run_ids = Array.new(10) { "run-#{Random.new.hex(8)}" }

      # Start all workflows
      run_ids.each do |rid|
        act = init_activation(rid, "SimpleWorkflow", [dc.to_payload("parallel")])
        bytes = runner.handle_activation(act.to_protobuf.to_slice)
        comp = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes))
        comp.successful.not_nil!.commands.not_nil!.any?(&.complete_workflow_execution).should be_true
      end

      runner.cached_size.should eq(0)
    end
  end

  # ── Context ────────────────────────────────────────────────────────────────

  describe "Workflow::Context" do
    it "raises when accessed outside a workflow fiber" do
      expect_raises(Temporalio::Error, /workflow/) do
        Temporalio::Workflow::Context.current
      end
    end

    it "provides deterministic now from activation timestamp" do
      run_id = "run-#{Random.new.hex(8)}"
      fixed_ts = 1_700_000_000_i64
      act = Coresdk::WorkflowActivation::WorkflowActivation.new(
        run_id: run_id,
        timestamp: Google::Protobuf::Timestamp.new(seconds: fixed_ts),
        jobs: [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
              workflow_type: "SimpleWorkflow",
              workflow_id: "wf-id",
              attempt: 1,
              arguments: [dc.to_payload("time-test")]
            )
          ),
        ]
      )

      wf = Temporalio::Internal::ConcreteWorkflowObject(SimpleWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(act, wf, dc, "default", "test-queue")
      inst.apply_activation(act)
      inst.complete?.should be_true
    end
  end
end

# ─── Determinism / replay correctness ────────────────────────────────────────

describe "Workflow determinism" do
  it "re-running the same activation sequence produces identical commands" do
    dc = Temporalio::DataConverter::DEFAULT

    # Capture commands from first run
    run_id_1 = "det-run-A"
    act1 = init_activation(run_id_1, "TimerWorkflow", [dc.to_payload(2_i64)])
    wf1 = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new
    inst1 = Temporalio::Internal::WorkflowInstance.new(act1, wf1, dc, "default", "test-queue")
    comp1a = inst1.apply_activation(act1)
    seq1 = comp1a.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq

    fire1 = followup_activation(run_id_1, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq1)
      ),
    ])
    comp1b = inst1.apply_activation(fire1)
    result1 = dc.from_payload(
      comp1b.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
      String
    )

    # Second run — same sequence
    run_id_2 = "det-run-B"
    act2 = init_activation(run_id_2, "TimerWorkflow", [dc.to_payload(2_i64)])
    wf2 = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new
    inst2 = Temporalio::Internal::WorkflowInstance.new(act2, wf2, dc, "default", "test-queue")
    comp2a = inst2.apply_activation(act2)
    seq2 = comp2a.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq

    fire2 = followup_activation(run_id_2, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq2)
      ),
    ])
    comp2b = inst2.apply_activation(fire2)
    result2 = dc.from_payload(
      comp2b.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
      String
    )

    result1.should eq(result2)
    seq1.should eq(seq2)
  end

  it "sequence numbers are independent per workflow instance" do
    dc = Temporalio::DataConverter::DEFAULT

    run_a = "seq-run-A"
    run_b = "seq-run-B"

    act_a = init_activation(run_a, "TimerWorkflow", [dc.to_payload(1_i64)])
    act_b = init_activation(run_b, "TimerWorkflow", [dc.to_payload(1_i64)])

    wf_a = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new
    wf_b = Temporalio::Internal::ConcreteWorkflowObject(UnitTimerWorkflow).new

    inst_a = Temporalio::Internal::WorkflowInstance.new(act_a, wf_a, dc, "default", "test-queue")
    inst_b = Temporalio::Internal::WorkflowInstance.new(act_b, wf_b, dc, "default", "test-queue")

    comp_a = inst_a.apply_activation(act_a)
    comp_b = inst_b.apply_activation(act_b)

    seq_a = comp_a.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq
    seq_b = comp_b.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq

    # Each instance starts sequence at 1
    seq_a.should eq(1_u32)
    seq_b.should eq(1_u32)
  end

  it "multiple workflow types run concurrently without state cross-contamination" do
    dc = Temporalio::DataConverter::DEFAULT
    runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
    runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(SimpleWorkflow).new)
    runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(UnitTimerWorkflow).new)

    # Start a timer workflow (leaves it in cache)
    timer_run = "timer-run-#{Random.new.hex(4)}"
    act_timer = init_activation(timer_run, "TimerWorkflow", [dc.to_payload(1_i64)])
    bytes_t = runner.handle_activation(act_timer.to_protobuf.to_slice)
    comp_t = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes_t))
    timer_seq = comp_t.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq
    runner.cached_size.should eq(1)

    # Run a simple workflow to completion (different type)
    simple_run = "simple-run-#{Random.new.hex(4)}"
    act_simple = init_activation(simple_run, "SimpleWorkflow", [dc.to_payload("concurrent")])
    bytes_s = runner.handle_activation(act_simple.to_protobuf.to_slice)
    comp_s = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes_s))
    result_s = dc.from_payload(
      comp_s.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
      String
    )
    result_s.should eq("Hello, concurrent!")
    runner.cached_size.should eq(1) # timer workflow still alive

    # Complete the timer workflow
    fire_act = followup_activation(timer_run, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: timer_seq)
      ),
    ])
    bytes_t2 = runner.handle_activation(fire_act.to_protobuf.to_slice)
    comp_t2 = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes_t2))
    comp_t2.successful.not_nil!.commands.not_nil!.any?(&.complete_workflow_execution).should be_true
    runner.cached_size.should eq(0)
  end
end
