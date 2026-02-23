require "../spec_helper"

# Stress test: many sequential timers (time-skipping simulation).
# Tests determinism under high timer volume.

class ChainedTimerWorkflow
  include Temporalio::Workflow
  workflow_name "ChainedTimerWorkflow"

  def execute(count : Int64) : Int64
    ctx = Temporalio::Workflow::Context.current
    count.times { ctx.sleep(1.second) }
    count
  end

end

private def timer_activation(run_id : String, jobs : Array(Coresdk::WorkflowActivation::WorkflowActivationJob), ts : Int64 = 0_i64)
  Coresdk::WorkflowActivation::WorkflowActivation.new(
    run_id: run_id,
    timestamp: Google::Protobuf::Timestamp.new(seconds: ts),
    jobs: jobs
  )
end

describe "Timer storm stress test" do
  it "handles 100 sequential timers correctly" do
    dc = Temporalio::DataConverter::DEFAULT
    count = 100_i64
    run_id = "timer-storm-#{Random.new.hex(6)}"
    base_ts = 1_700_000_000_i64

    init_act = timer_activation(run_id, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
          workflow_type: "ChainedTimerWorkflow",
          workflow_id: "wf-timer-storm",
          attempt: 1,
          arguments: [dc.to_payload(count)]
        )
      ),
    ], base_ts)

    wf = Temporalio::Internal::ConcreteWorkflowObject(ChainedTimerWorkflow).new
    inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")

    comp = inst.apply_activation(init_act)
    last_result : Int64? = nil

    count.times do |i|
      inst.complete?.should be_false
      seq = comp.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq.not_nil!
      ts = base_ts + i + 1
      comp = inst.apply_activation(timer_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq)
        ),
      ], ts))
    end

    inst.complete?.should be_true
    result = dc.from_payload(
      comp.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
      Int64
    )
    result.should eq(count)
  end

  it "sequence numbers monotonically increase across 100 timers" do
    dc = Temporalio::DataConverter::DEFAULT
    count = 100_i64
    run_id = "timer-seq-#{Random.new.hex(6)}"

    init_act = timer_activation(run_id, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
          workflow_type: "ChainedTimerWorkflow",
          workflow_id: "wf-timer-seq",
          attempt: 1,
          arguments: [dc.to_payload(count)]
        )
      ),
    ])

    wf = Temporalio::Internal::ConcreteWorkflowObject(ChainedTimerWorkflow).new
    inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")

    comp = inst.apply_activation(init_act)
    last_seq = 0_u32

    count.times do
      seq = comp.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq.not_nil!
      seq.should be > last_seq
      last_seq = seq

      comp = inst.apply_activation(timer_activation(run_id, [
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq)
        ),
      ]))
    end

    inst.complete?.should be_true
  end

  it "handles 20 concurrent timer workflows independently" do
    dc = Temporalio::DataConverter::DEFAULT
    done = Channel(Bool).new(20)

    20.times do |i|
      spawn do
        run_id = "concurrent-timer-#{i}-#{Random.new.hex(4)}"
        steps = (i % 5 + 1).to_i64  # 1-5 timers per workflow

        init_act = timer_activation(run_id, [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
              workflow_type: "ChainedTimerWorkflow",
              workflow_id: "wf-t-#{i}",
              attempt: 1,
              arguments: [dc.to_payload(steps)]
            )
          ),
        ])

        wf = Temporalio::Internal::ConcreteWorkflowObject(ChainedTimerWorkflow).new
        inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
        comp = inst.apply_activation(init_act)

        steps.times do
          seq = comp.successful.not_nil!.commands.not_nil![0].start_timer.not_nil!.seq.not_nil!
          comp = inst.apply_activation(timer_activation(run_id, [
            Coresdk::WorkflowActivation::WorkflowActivationJob.new(
              fire_timer: Coresdk::WorkflowActivation::FireTimer.new(seq: seq)
            ),
          ]))
        end

        result = dc.from_payload(
          comp.successful.not_nil!.commands.not_nil![0].complete_workflow_execution.not_nil!.result.not_nil!,
          Int64
        )
        done.send(result == steps)
      end
    end

    successes = 20.times.map { done.receive }.to_a
    successes.all?.should be_true
  end
end
