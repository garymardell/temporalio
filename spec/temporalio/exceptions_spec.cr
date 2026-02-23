require "../spec_helper"
require "../../src/temporalio/internal/failure_converter"

describe Temporalio::Internal::FailureConverter do
  dc = Temporalio::DataConverter::DEFAULT

  describe ".from_failure" do
    it "converts ApplicationFailureInfo to ApplicationError" do
      failure = Temporal::Api::Failure::V1::Failure.new(
        message: "oops",
        application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
          type: "MyError",
          non_retryable: true
        )
      )
      err = Temporalio::Internal::FailureConverter.from_failure(failure, dc)
      err.should be_a(Temporalio::ApplicationError)
      app = err.as(Temporalio::ApplicationError)
      app.message.should eq("oops")
      app.type.should eq("MyError")
      app.non_retryable.should be_true
    end

    it "converts TimeoutFailureInfo to TimeoutError" do
      failure = Temporal::Api::Failure::V1::Failure.new(
        message: "timed out",
        timeout_failure_info: Temporal::Api::Failure::V1::TimeoutFailureInfo.new(
          timeout_type: 1  # START_TO_CLOSE
        )
      )
      err = Temporalio::Internal::FailureConverter.from_failure(failure, dc)
      err.should be_a(Temporalio::TimeoutError)
      err.as(Temporalio::TimeoutError).timeout_type.should eq(1)
      err.as(Temporalio::TimeoutError).timeout_type_name.should eq("START_TO_CLOSE")
    end

    it "converts CanceledFailureInfo to CancelledError" do
      failure = Temporal::Api::Failure::V1::Failure.new(
        message: "cancelled",
        canceled_failure_info: Temporal::Api::Failure::V1::CanceledFailureInfo.new
      )
      err = Temporalio::Internal::FailureConverter.from_failure(failure, dc)
      err.should be_a(Temporalio::CancelledError)
    end

    it "converts TerminatedFailureInfo to TerminatedError" do
      failure = Temporal::Api::Failure::V1::Failure.new(
        message: "terminated",
        terminated_failure_info: Temporal::Api::Failure::V1::TerminatedFailureInfo.new
      )
      err = Temporalio::Internal::FailureConverter.from_failure(failure, dc)
      err.should be_a(Temporalio::TerminatedError)
    end

    it "converts ServerFailureInfo to ServerError" do
      failure = Temporal::Api::Failure::V1::Failure.new(
        message: "server error",
        server_failure_info: Temporal::Api::Failure::V1::ServerFailureInfo.new(non_retryable: true)
      )
      err = Temporalio::Internal::FailureConverter.from_failure(failure, dc)
      err.should be_a(Temporalio::ServerError)
      err.as(Temporalio::ServerError).non_retryable.should be_true
    end

    it "converts ActivityFailureInfo to ActivityError" do
      failure = Temporal::Api::Failure::V1::Failure.new(
        message: "activity failed",
        activity_failure_info: Temporal::Api::Failure::V1::ActivityFailureInfo.new(
          scheduled_event_id: 5_i64,
          started_event_id: 6_i64,
          activity_type: Temporal::Api::Common::V1::ActivityType.new(name: "MyActivity"),
          activity_id: "act-1",
          retry_state: 1
        )
      )
      err = Temporalio::Internal::FailureConverter.from_failure(failure, dc)
      err.should be_a(Temporalio::ActivityError)
      act = err.as(Temporalio::ActivityError)
      act.activity_type.should eq("MyActivity")
      act.activity_id.should eq("act-1")
      act.scheduled_event_id.should eq(5_i64)
    end

    it "builds nested cause chain" do
      inner = Temporal::Api::Failure::V1::Failure.new(
        message: "root cause",
        application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(type: "RootError")
      )
      outer = Temporal::Api::Failure::V1::Failure.new(
        message: "outer",
        activity_failure_info: Temporal::Api::Failure::V1::ActivityFailureInfo.new(
          activity_type: Temporal::Api::Common::V1::ActivityType.new(name: "Act"),
          activity_id: "1",
          scheduled_event_id: 1_i64,
          started_event_id: 2_i64
        ),
        cause: inner
      )
      err = Temporalio::Internal::FailureConverter.from_failure(outer, dc)
      err.cause.should be_a(Temporalio::ApplicationError)
      err.cause.as(Temporalio::ApplicationError).type.should eq("RootError")
    end
  end

  describe ".to_failure" do
    it "converts ApplicationError to ApplicationFailureInfo" do
      err = Temporalio::ApplicationError.new("oops", type: "MyError", non_retryable: true)
      failure = Temporalio::Internal::FailureConverter.to_failure(err, dc)
      failure.message.should eq("oops")
      info = failure.application_failure_info.not_nil!
      info.type.should eq("MyError")
      info.non_retryable.should be_true
    end

    it "converts CancelledError to CanceledFailureInfo" do
      err = Temporalio::CancelledError.new("nope")
      failure = Temporalio::Internal::FailureConverter.to_failure(err, dc)
      failure.canceled_failure_info.should_not be_nil
    end

    it "converts TerminatedError to TerminatedFailureInfo" do
      err = Temporalio::TerminatedError.new
      failure = Temporalio::Internal::FailureConverter.to_failure(err, dc)
      failure.terminated_failure_info.should_not be_nil
    end

    it "converts unknown exception to non-retryable ApplicationError" do
      err = RuntimeError.new("boom")
      failure = Temporalio::Internal::FailureConverter.to_failure(err, dc)
      info = failure.application_failure_info.not_nil!
      info.non_retryable.should be_true
    end

    it "round-trips ApplicationError through proto" do
      original = Temporalio::ApplicationError.new("test", type: "TestError", non_retryable: false)
      failure = Temporalio::Internal::FailureConverter.to_failure(original, dc)

      # Encode/decode through protobuf
      encoded = failure.to_protobuf.to_slice
      decoded_failure = Temporal::Api::Failure::V1::Failure.from_protobuf(IO::Memory.new(encoded))

      restored = Temporalio::Internal::FailureConverter.from_failure(decoded_failure, dc)
      restored.should be_a(Temporalio::ApplicationError)
      restored.message.should eq("test")
      restored.as(Temporalio::ApplicationError).type.should eq("TestError")
    end
  end
end
