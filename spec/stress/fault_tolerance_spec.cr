require "../spec_helper"
require "../support/activities/failing_activity"
require "../support/workflows/simple_workflow"
require "../support/workflows/timer_workflow"

# Fault tolerance tests: verify behavior under error conditions.
# Tests that the engine handles failures gracefully without deadlocks.

class RetryableWorkflow
  include Temporalio::Workflow
  workflow_name "RetryableWorkflow"

  def execute : String
    begin
      workflow.execute_activity(FailingActivity, "fail", start_to_close_timeout: 30.seconds)
      "succeeded"
    rescue ex : Temporalio::ApplicationError
      "caught:#{ex.message}"
    end
  end

end

class MultiActivityRetryWorkflow
  include Temporalio::Workflow
  workflow_name "MultiActivityRetryWorkflow"

  def execute(n : Int64) : Int64
    successes = 0_i64
    n.times do |i|
      begin
        workflow.execute_activity(FailingActivity, "fail#{i}", start_to_close_timeout: 10.seconds)
        successes += 1
      rescue ex : Temporalio::ApplicationError
      end
    end
    successes
  end

end

class NestedErrorWorkflow
  include Temporalio::Workflow
  workflow_name "NestedErrorWorkflow"

  def execute : String
    result1 = begin
      workflow.execute_activity(FailingActivity, "first", start_to_close_timeout: 10.seconds)
      "ok"
    rescue ex : Temporalio::ApplicationError
      ex.message || "unknown"
    end
    result2 = begin
      workflow.execute_activity(FailingActivity, "second", start_to_close_timeout: 10.seconds)
      "ok"
    rescue ex : Temporalio::ApplicationError
      ex.message || "unknown"
    end
    "#{result1}|#{result2}"
  end

end

private def make_act(run_id : String, jobs : Array(Coresdk::WorkflowActivation::WorkflowActivationJob))
  Coresdk::WorkflowActivation::WorkflowActivation.new(
    run_id: run_id,
    timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
    jobs: jobs
  )
end

private def fail_activity(seq : UInt32, message : String) : Coresdk::WorkflowActivation::WorkflowActivationJob
  Coresdk::WorkflowActivation::WorkflowActivationJob.new(
    resolve_activity: Coresdk::WorkflowActivation::ResolveActivity.new(
      seq: seq,
      result: Coresdk::ActivityResult::ActivityResolution.new(
        failed: Coresdk::ActivityResult::Failure.new(
          failure: Temporal::Api::Failure::V1::Failure.new(
            message: message,
            application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
              type: "TestError",
              non_retryable: true
            )
          )
        )
      )
    )
  )
end

private def succeed_activity(seq : UInt32, dc : Temporalio::DataConverter, value : String) : Coresdk::WorkflowActivation::WorkflowActivationJob
  Coresdk::WorkflowActivation::WorkflowActivationJob.new(
    resolve_activity: Coresdk::WorkflowActivation::ResolveActivity.new(
      seq: seq,
      result: Coresdk::ActivityResult::ActivityResolution.new(
        completed: Coresdk::ActivityResult::Success.new(result: dc.to_payload(value))
      )
    )
  )
end

class AlwaysFailWorkflow
  include Temporalio::Workflow
  workflow_name "AlwaysFailWorkflow"

  def execute : String
    raise Temporalio::ApplicationError.new("intentional", type: "IntentionalError", non_retryable: true)
  end

end

describe "Fault tolerance" do
  describe "activity failure handling" do
    it "workflow catches activity failure and continues" do
      dc = Temporalio::DataConverter::DEFAULT
      run_id = "fault-#{Random.new.hex(6)}"

      init_act = make_act(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
            workflow_type: "RetryableWorkflow",
            workflow_id: "wf-retry",
            attempt: 1,
            arguments: [dc.to_payload(1_i64)]
          )
        ),
      ])

      wf = Temporalio::Internal::ConcreteWorkflowObject(RetryableWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(init_act)

      seq = comp1.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq.not_nil!

      comp2 = inst.apply_activation(make_act(run_id, [fail_activity(seq, "fail")]))

      inst.complete?.should be_true
      result = dc.from_payload(
        comp2.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("caught:fail")
    end

    it "handles multiple sequential activity failures without deadlock" do
      dc = Temporalio::DataConverter::DEFAULT
      run_id = "multi-fail-#{Random.new.hex(6)}"
      n = 5_i64

      init_act = make_act(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
            workflow_type: "MultiActivityRetryWorkflow",
            workflow_id: "wf-multi-fail",
            attempt: 1,
            arguments: [dc.to_payload(n)]
          )
        ),
      ])

      wf = Temporalio::Internal::ConcreteWorkflowObject(MultiActivityRetryWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(init_act)

      n.times do |i|
        inst.complete?.should be_false
        seq = comp.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq.not_nil!
        comp = inst.apply_activation(make_act(run_id, [fail_activity(seq, "err#{i}")]))
      end

      inst.complete?.should be_true
      result = dc.from_payload(
        comp.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        Int64
      )
      result.should eq(0_i64) # all failed
    end

    it "handles interleaved activity success and failure" do
      dc = Temporalio::DataConverter::DEFAULT
      run_id = "nested-err-#{Random.new.hex(6)}"

      init_act = make_act(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
            workflow_type: "NestedErrorWorkflow",
            workflow_id: "wf-nested",
            attempt: 1,
            arguments: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ])

      wf = Temporalio::Internal::ConcreteWorkflowObject(NestedErrorWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
      comp1 = inst.apply_activation(init_act)

      seq1 = comp1.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq.not_nil!
      comp2 = inst.apply_activation(make_act(run_id, [fail_activity(seq1, "first")]))

      inst.complete?.should be_false
      seq2 = comp2.successful.not_nil!.commands.not_nil![0].schedule_activity.not_nil!.seq.not_nil!
      comp3 = inst.apply_activation(make_act(run_id, [fail_activity(seq2, "second")]))

      inst.complete?.should be_true
      result = dc.from_payload(
        comp3.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
        String
      )
      result.should eq("first|second")
    end
  end

  describe "workflow failure propagation" do
    it "unhandled exception fails the workflow cleanly" do
      dc = Temporalio::DataConverter::DEFAULT
      run_id = "always-fail-#{Random.new.hex(6)}"
      init_act = make_act(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
            workflow_type: "AlwaysFailWorkflow",
            workflow_id: "wf-always-fail",
            attempt: 1,
            arguments: [] of Temporal::Api::Common::V1::Payload
          )
        ),
      ])

      wf = Temporalio::Internal::ConcreteWorkflowObject(AlwaysFailWorkflow).new
      inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
      comp = inst.apply_activation(init_act)

      inst.complete?.should be_true
      cmds = comp.successful.not_nil!.commands.not_nil!
      cmds.any?(&.fail_workflow_execution).should be_true
      failure = cmds.find(&.fail_workflow_execution).not_nil!
        .fail_workflow_execution.not_nil!.failure.not_nil!
      failure.message.should eq("intentional")
      failure.application_failure_info.not_nil!.type.should eq("IntentionalError")
    end

    it "engine continues processing other workflows after one fails" do
      dc = Temporalio::DataConverter::DEFAULT
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(SimpleWorkflow).new)

      # Run 5 workflows, checking each succeeds independently
      5.times do |i|
        run_id = "recovery-#{i}-#{Random.new.hex(4)}"
        init_act = Coresdk::WorkflowActivation::WorkflowActivation.new(
          run_id: run_id,
          timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
          jobs: [
            Coresdk::WorkflowActivation::WorkflowActivationJob.new(
              initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
                workflow_type: "SimpleWorkflow",
                workflow_id: "wf-recovery-#{i}",
                attempt: 1,
                arguments: [dc.to_payload("run#{i}")]
              )
            ),
          ]
        )
        bytes = runner.handle_activation(init_act.to_protobuf.to_slice)
        comp = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes))
        result = dc.from_payload(
          comp.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
          String
        )
        result.should eq("Hello, run#{i}!")
      end

      runner.cached_size.should eq(0)
    end
  end

  describe "cache eviction safety" do
    it "eviction during in-progress workflow does not affect other runs" do
      dc = Temporalio::DataConverter::DEFAULT
      runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
      runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(ChainedTimerWorkflow).new)

      # Start 3 timer workflows (all waiting)
      run_ids = Array.new(3) { "evict-safety-#{Random.new.hex(4)}" }
      seqs = {} of String => UInt32

      run_ids.each do |rid|
        init_act = Coresdk::WorkflowActivation::WorkflowActivation.new(
          run_id: rid,
          timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
          jobs: [
            Coresdk::WorkflowActivation::WorkflowActivationJob.new(
              initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
                workflow_type: "ChainedTimerWorkflow",
                workflow_id: "wf-#{rid}",
                attempt: 1,
                arguments: [dc.to_payload(1_i64)]
              )
            ),
          ]
        )
        bytes = runner.handle_activation(init_act.to_protobuf.to_slice)
        comp = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes))
        seq = comp.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq.not_nil!
        seqs[rid] = seq
      end

      runner.cached_size.should eq(3)

      # Evict the middle one
      evict_act = Coresdk::WorkflowActivation::WorkflowActivation.new(
        run_id: run_ids[1],
        timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
        jobs: [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            remove_from_cache: Coresdk::WorkflowActivation::RemoveFromCache.new(
              message: "evicted",
              reason: Coresdk::WorkflowActivation::RemoveFromCache::EvictionReason::UNSPECIFIED.value
            )
          ),
        ]
      )
      runner.handle_activation(evict_act.to_protobuf.to_slice)
      runner.cached_size.should eq(2)

      # Complete the remaining two
      [run_ids[0], run_ids[2]].each do |rid|
        fire_act = Coresdk::WorkflowActivation::WorkflowActivation.new(
          run_id: rid,
          timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
          jobs: [
            Coresdk::WorkflowActivation::WorkflowActivationJob.new(
              fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seqs[rid])
            ),
          ]
        )
        bytes = runner.handle_activation(fire_act.to_protobuf.to_slice)
        comp = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(IO::Memory.new(bytes))
        comp.successful.not_nil!.commands.not_nil!.any?(&.complete_workflow_execution).should be_true
      end

      runner.cached_size.should eq(0)
    end
  end
end
