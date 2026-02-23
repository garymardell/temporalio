require "../spec_helper"

# Stress test: run many workflow instances concurrently via WorkflowRunner.
# This tests the fiber coordination under load without needing a real server.

class StressWorkflow
  include Temporalio::Workflow
  workflow_name "StressWorkflow"

  def execute(n : Int64) : Int64
    n * 2_i64
  end

end

private def make_activation(run_id : String, n : Int64) : Bytes
  dc = Temporalio::DataConverter::DEFAULT
  init_job = Coresdk::WorkflowActivation::WorkflowActivationJob.new(
    initialize_workflow: Coresdk::WorkflowActivation::InitializeWorkflow.new(
      workflow_type: "StressWorkflow",
      workflow_id: "wf-#{run_id}",
      attempt: 1,
      arguments: [dc.to_payload(n)]
    )
  )
  Coresdk::WorkflowActivation::WorkflowActivation.new(
    run_id: run_id,
    timestamp: Google::Protobuf::Timestamp.new(seconds: Time.utc.to_unix),
    jobs: [init_job]
  ).to_protobuf.to_slice
end

describe "Concurrent workflow stress test" do
  it "handles 500 sequential workflow completions correctly" do
    dc = Temporalio::DataConverter::DEFAULT
    runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
    runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(StressWorkflow).new)

    results = [] of Int64
    500.times do |i|
      run_id = "stress-run-#{i}"
      activation_bytes = make_activation(run_id, i.to_i64)

      completion_bytes = runner.handle_activation(activation_bytes)
      completion = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(
        IO::Memory.new(completion_bytes)
      )

      cmds = completion.successful.not_nil!.commands || [] of Coresdk::WorkflowCommands::WorkflowCommand
      complete_cmd = cmds.find(&.complete_workflow_execution)
      next unless complete_cmd

      payload = complete_cmd.not_nil!.complete_workflow_execution.not_nil!.result
      results << dc.from_payload(payload.not_nil!, Int64) if payload
    end

    results.size.should eq(500)
    runner.cached_size.should eq(0)

    results.each_with_index do |result, i|
      result.should eq(i * 2)
    end
  end

  it "handles 100 concurrent workflow completions via spawn" do
    dc = Temporalio::DataConverter::DEFAULT
    done = Channel(Int64?).new(100)

    100.times do |i|
      spawn do
        runner = Temporalio::Internal::WorkflowRunner.new(dc, "default", "test-queue")
        runner.register(Temporalio::Internal::ConcreteWorkflowDefinition(StressWorkflow).new)

        activation_bytes = make_activation("concurrent-#{i}", i.to_i64)
        completion_bytes = runner.handle_activation(activation_bytes)
        completion = Coresdk::WorkflowCompletion::WorkflowActivationCompletion.from_protobuf(
          IO::Memory.new(completion_bytes)
        )

        cmds = completion.successful.not_nil!.commands || [] of Coresdk::WorkflowCommands::WorkflowCommand
        cmd = cmds.find(&.complete_workflow_execution)
        if cmd && (payload = cmd.not_nil!.complete_workflow_execution.not_nil!.result)
          done.send(dc.from_payload(payload, Int64))
        else
          done.send(nil)
        end
      end
    end

    results = 100.times.map { done.receive }.to_a
    successful = results.compact
    successful.size.should eq(100)
  end
end
