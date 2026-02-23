require "../spec_helper"

class LoggingInterceptor < Temporalio::Interceptor::ClientInterceptor
  getter log : Array(String)

  def initialize
    @log = [] of String
  end

  def start_workflow(
    input : Temporalio::Interceptor::StartWorkflowInput,
    next_fn : Proc(Temporalio::Interceptor::StartWorkflowInput, String)
  ) : String
    @log << "before:#{input.workflow_type}"
    result = next_fn.call(input)
    @log << "after:#{result}"
    result
  end
end

describe Temporalio::Interceptor::ClientInterceptor do
  it "passes through by default (start_workflow)" do
    interceptor = Temporalio::Interceptor::ClientInterceptor.new
    called_with = nil

    next_fn = Proc(Temporalio::Interceptor::StartWorkflowInput, String).new do |input|
      called_with = input.workflow_type
      "test-run-id"
    end

    input = Temporalio::Interceptor::StartWorkflowInput.new(
      workflow_type: "MyWorkflow",
      args: [] of Temporal::Api::Common::V1::Payload,
      workflow_id: "wf-1",
      task_queue: "my-queue"
    )

    result = interceptor.start_workflow(input, next_fn)
    result.should eq("test-run-id")
    called_with.should eq("MyWorkflow")
  end

  it "passes through by default (signal_workflow)" do
    interceptor = Temporalio::Interceptor::ClientInterceptor.new
    called = false

    next_fn = Proc(Temporalio::Interceptor::SignalWorkflowInput, Nil).new do |_input|
      called = true
      nil
    end

    input = Temporalio::Interceptor::SignalWorkflowInput.new(
      workflow_id: "wf-1",
      run_id: nil,
      signal: "my-signal",
      args: [] of Temporal::Api::Common::V1::Payload
    )

    interceptor.signal_workflow(input, next_fn)
    called.should be_true
  end

  it "can be subclassed to intercept calls" do
    interceptor = LoggingInterceptor.new
    next_fn = Proc(Temporalio::Interceptor::StartWorkflowInput, String).new { |_| "run-xyz" }

    input = Temporalio::Interceptor::StartWorkflowInput.new(
      workflow_type: "TestWF",
      args: [] of Temporal::Api::Common::V1::Payload,
      workflow_id: "wf-1",
      task_queue: "q"
    )

    interceptor.start_workflow(input, next_fn)
    interceptor.log.should eq(["before:TestWF", "after:run-xyz"])
  end
end

describe Temporalio::Interceptor::WorkerInterceptor do
  it "passes through by default (execute_activity)" do
    dc = Temporalio::DataConverter::DEFAULT
    interceptor = Temporalio::Interceptor::WorkerInterceptor.new
    called = false

    expected_result = dc.to_payload("result")
    next_fn = Proc(Temporalio::Interceptor::ExecuteActivityInput, Temporal::Api::Common::V1::Payload?).new do |_|
      called = true
      expected_result
    end

    input = Temporalio::Interceptor::ExecuteActivityInput.new(
      activity_type: "MyActivity",
      args: [] of Temporal::Api::Common::V1::Payload,
      task_token: Bytes.empty
    )

    result = interceptor.execute_activity(input, next_fn)
    result.should_not be_nil
    dc.from_payload(result.not_nil!, String).should eq("result")
    called.should be_true
  end

  it "passes through by default (start_timer)" do
    interceptor = Temporalio::Interceptor::WorkerInterceptor.new
    called = false

    next_fn = Proc(Temporalio::Interceptor::StartTimerInput, Nil).new do |_|
      called = true
      nil
    end

    input = Temporalio::Interceptor::StartTimerInput.new(duration: 5.seconds)
    interceptor.start_timer(input, next_fn)
    called.should be_true
  end
end
