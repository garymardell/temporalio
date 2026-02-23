require "../spec_helper"

# Stress test: many signals to a single workflow instance.

class SignalAccumulatorWorkflow
  include Temporalio::Workflow
  workflow_name "SignalAccumulatorWorkflow"

  @total : Int64 = 0_i64
  @done : Bool = false

  def execute : Int64
    ctx = Temporalio::Workflow::Context.current
    ctx.wait_condition { @done }
    @total
  end


  workflow_signal("add", Int64) do |n|
    @total += n
  end

  workflow_signal("done") do
    @done = true
  end
end

private def make_activation(run_id : String, jobs : Array(Coresdk::WorkflowActivation::WorkflowActivationJob), ts : Int64 = Time.utc.to_unix)
  Coresdk::WorkflowActivation::WorkflowActivation.new(
    run_id: run_id,
    timestamp: Google::Protobuf::Timestamp.new(seconds: ts),
    jobs: jobs
  )
end

describe "Signal storm stress test" do
  it "handles 200 sequential signals to a single workflow" do
    dc = Temporalio::DataConverter::DEFAULT
    run_id = "signal-storm-#{Random.new.hex(6)}"

    init_act = make_activation(run_id, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
          workflow_type: "SignalAccumulatorWorkflow",
          workflow_id: "wf-signal-storm",
          attempt: 1,
          arguments: [dc.to_payload(200_i64)]
        )
      ),
    ])

    wf = Temporalio::Internal::ConcreteWorkflowObject(SignalAccumulatorWorkflow).new
    inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
    inst.apply_activation(init_act)
    inst.complete?.should be_false

    # Send 200 signals in batches of 10
    20.times do |batch|
      signal_jobs = (0...10).map do |i|
        Coresdk::WorkflowActivation::WorkflowActivationJob.new(
          signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
            signal_name: "add",
            input: [dc.to_payload(1_i64)]
          )
        )
      end
      inst.apply_activation(make_activation(run_id, signal_jobs))
      inst.complete?.should be_false
    end

    # Send done signal
    done_act = make_activation(run_id, [
      Coresdk::WorkflowActivation::WorkflowActivationJob.new(
        signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
          signal_name: "done",
          input: [] of Temporal::Api::Common::V1::Payload
        )
      ),
    ])
    comp = inst.apply_activation(done_act)

    inst.complete?.should be_true
    cmds = comp.successful.not_nil!.commands.not_nil!
    result = dc.from_payload(
      cmds.find(&.complete_workflow_execution).not_nil!
        .complete_workflow_execution.not_nil!.result.not_nil!,
      Int64
    )
    result.should eq(200_i64)
  end

  it "handles 50 concurrent independent signal workflows" do
    dc = Temporalio::DataConverter::DEFAULT
    done = Channel(Int64?).new(50)

    50.times do |i|
      spawn do
        run_id = "storm-concurrent-#{i}-#{Random.new.hex(4)}"

        init_act = make_activation(run_id, [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
              workflow_type: "SignalAccumulatorWorkflow",
              workflow_id: "wf-#{i}",
              attempt: 1,
              arguments: [dc.to_payload(5_i64)]
            )
          ),
        ])

        wf = Temporalio::Internal::ConcreteWorkflowObject(SignalAccumulatorWorkflow).new
        inst = Temporalio::Internal::WorkflowInstance.new(init_act, wf, dc, "default", "test-queue")
        inst.apply_activation(init_act)

        # 5 signals
        5.times do
          inst.apply_activation(make_activation(run_id, [
            Coresdk::WorkflowActivation::WorkflowActivationJob.new(
              signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
                signal_name: "add",
                input: [dc.to_payload(2_i64)]
              )
            ),
          ]))
        end

        comp = inst.apply_activation(make_activation(run_id, [
          Coresdk::WorkflowActivation::WorkflowActivationJob.new(
            signal_workflow: Coresdk::WorkflowActivation::SignalWorkflow.new(
              signal_name: "done",
              input: [] of Temporal::Api::Common::V1::Payload
            )
          ),
        ]))

        cmds = comp.successful.not_nil!.commands.not_nil!
        payload = cmds.find(&.complete_workflow_execution).try(&.complete_workflow_execution.try(&.result))
        done.send(payload ? dc.from_payload(payload, Int64) : nil)
      end
    end

    results = 50.times.map { done.receive }.to_a
    results.compact.size.should eq(50)
    results.each { |r| r.should eq(10_i64) }
  end
end
