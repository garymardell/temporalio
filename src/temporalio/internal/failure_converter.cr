require "../exceptions"
require "../data_converter"
require "./proto"

module Temporalio
  module Internal
    # Converts between proto Failure messages and Crystal exceptions.
    module FailureConverter
      # Convert a proto Failure to a Crystal exception.
      # Recursively builds the cause chain.
      def self.from_failure(
        failure : Temporal::Api::Failure::V1::Failure,
        converter : DataConverter = DataConverter::DEFAULT
      ) : FailureError
        cause = failure.cause ? from_failure(failure.cause.not_nil!, converter) : nil
        message = failure.message || ""

        if info = failure.application_failure_info
          details = decode_payloads(info.details, converter)
          nrd = info.next_retry_delay.try { |d| Time::Span.new(seconds: d.seconds || 0_i64, nanoseconds: d.nanos || 0) }
          return ApplicationError.new(
            message: message,
            type: info.type,
            non_retryable: info.non_retryable || false,
            details: details,
            next_retry_delay: nrd,
            cause: cause
          )
        end

        if info = failure.timeout_failure_info
          hb_details = decode_payloads(info.last_heartbeat_details, converter)
          return TimeoutError.new(
            message: message.empty? ? "Workflow/activity timed out" : message,
            timeout_type: info.timeout_type || 0,
            last_heartbeat_details: hb_details,
            cause: cause
          )
        end

        if info = failure.canceled_failure_info
          details = decode_payloads(info.details, converter)
          return CancelledError.new(
            message: message.empty? ? "Cancelled" : message,
            details: details,
            cause: cause
          )
        end

        if failure.terminated_failure_info
          return TerminatedError.new(
            message: message.empty? ? "Terminated" : message,
            cause: cause
          )
        end

        if info = failure.server_failure_info
          return ServerError.new(
            message: message,
            non_retryable: info.non_retryable || false,
            cause: cause
          )
        end

        if info = failure.activity_failure_info
          activity_type = info.activity_type.try(&.name) || ""
          return ActivityError.new(
            message: message,
            scheduled_event_id: info.scheduled_event_id || 0_i64,
            started_event_id: info.started_event_id || 0_i64,
            activity_type: activity_type,
            activity_id: info.activity_id || "",
            retry_state: info.retry_state || 0,
            cause: cause
          )
        end

        if info = failure.child_workflow_execution_failure_info
          wf_execution = info.workflow_execution
          wf_type = info.workflow_type
          return ChildWorkflowError.new(
            message: message,
            namespace: info.namespace || "",
            workflow_id: wf_execution.try(&.workflow_id) || "",
            run_id: wf_execution.try(&.run_id) || "",
            workflow_type: wf_type.try(&.name) || "",
            retry_state: info.retry_state || 0,
            cause: cause
          )
        end

        # Fallback: treat as ApplicationError
        ApplicationError.new(
          message: message,
          non_retryable: false,
          cause: cause
        )
      end

      # Convert a Crystal exception to a proto Failure.
      # Unknown exceptions become non-retryable ApplicationFailureInfo.
      def self.to_failure(
        error : Exception,
        converter : DataConverter = DataConverter::DEFAULT
      ) : Temporal::Api::Failure::V1::Failure
        cause_failure = if error.is_a?(FailureError) && error.cause
                          to_failure(error.cause.not_nil!, converter)
                        end

        case error
        when ApplicationError
          app_info = Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
            type: error.type,
            non_retryable: error.non_retryable,
            details: encode_payloads(error.details, converter),
            next_retry_delay: error.next_retry_delay.try { |span|
              Google::Protobuf::Duration.new(
                seconds: span.total_seconds.to_i64,
                nanos: span.nanoseconds
              )
            }
          )
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            application_failure_info: app_info,
            cause: cause_failure
          )
        when CancelledError
          details = encode_payloads(error.details, converter)
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            canceled_failure_info: Temporal::Api::Failure::V1::CanceledFailureInfo.new(details: details),
            cause: cause_failure
          )
        when TerminatedError
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            terminated_failure_info: Temporal::Api::Failure::V1::TerminatedFailureInfo.new,
            cause: cause_failure
          )
        when TimeoutError
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            timeout_failure_info: Temporal::Api::Failure::V1::TimeoutFailureInfo.new(
              timeout_type: error.timeout_type
            ),
            cause: cause_failure
          )
        when ServerError
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            server_failure_info: Temporal::Api::Failure::V1::ServerFailureInfo.new(
              non_retryable: error.non_retryable
            ),
            cause: cause_failure
          )
        when ActivityError
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            activity_failure_info: Temporal::Api::Failure::V1::ActivityFailureInfo.new(
              scheduled_event_id: error.scheduled_event_id,
              started_event_id: error.started_event_id,
              activity_type: Temporal::Api::Common::V1::ActivityType.new(name: error.activity_type),
              activity_id: error.activity_id,
              retry_state: error.retry_state
            ),
            cause: cause_failure
          )
        when ChildWorkflowError
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || "",
            child_workflow_execution_failure_info: Temporal::Api::Failure::V1::ChildWorkflowExecutionFailureInfo.new(
              namespace: error.namespace,
              workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
                workflow_id: error.workflow_id,
                run_id: error.run_id
              ),
              workflow_type: Temporal::Api::Common::V1::WorkflowType.new(name: error.workflow_type),
              retry_state: error.retry_state
            ),
            cause: cause_failure
          )
        else
          # Any unknown exception → non-retryable ApplicationError
          Temporal::Api::Failure::V1::Failure.new(
            message: error.message || error.class.name,
            application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
              type: error.class.name,
              non_retryable: true
            ),
            cause: cause_failure
          )
        end
      end

      # Decode a Payloads message to raw JSON strings (one per payload).
      private def self.decode_payloads(payloads_msg : Temporal::Api::Common::V1::Payloads?, converter : DataConverter) : Array(String)
        return [] of String if payloads_msg.nil?
        converter.from_payloads_message(payloads_msg).compact
      end

      # Encode raw JSON strings back to a Payloads message.
      # Each string is stored as a pre-encoded json/plain payload.
      private def self.encode_payloads(values : Array(String), converter : DataConverter) : Temporal::Api::Common::V1::Payloads?
        return nil if values.empty?
        payloads = values.map do |json_str|
          enc_entry = Temporal::Api::Common::V1::StringBytesEntry.new(
            key: "encoding",
            value: Temporalio::JSON_ENCODING.to_slice
          )
          Temporal::Api::Common::V1::Payload.new(
            metadata: [enc_entry],
            data: json_str.to_slice
          )
        end
        Temporal::Api::Common::V1::Payloads.new(payloads: payloads)
      end
    end
  end
end
