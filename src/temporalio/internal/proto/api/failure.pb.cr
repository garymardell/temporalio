require "protobuf"
require "../google_wkt.pb"

require "./enums.pb"
require "./common.pb"

# Hand-written Crystal proto bindings for temporal.api.failure.v1.message.proto

module Temporal
  module Api
    module Failure
      module V1
        struct ApplicationFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :type, :string, 1
            optional :non_retryable, :bool, 2
            optional :details, Temporal::Api::Common::V1::Payloads, 3
            optional :next_retry_delay, Google::Protobuf::Duration, 4
            optional :category, :int32, 5
          end
        end

        struct TimeoutFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :timeout_type, :int32, 1
            optional :last_heartbeat_details, Temporal::Api::Common::V1::Payloads, 2
          end
        end

        struct CanceledFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :details, Temporal::Api::Common::V1::Payloads, 1
          end
        end

        struct TerminatedFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
          end
        end

        struct ServerFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :non_retryable, :bool, 1
          end
        end

        struct ResetWorkflowFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :last_heartbeat_details, Temporal::Api::Common::V1::Payloads, 1
          end
        end

        struct ActivityFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :scheduled_event_id, :int64, 1
            optional :started_event_id, :int64, 2
            optional :identity, :string, 3
            optional :activity_type, Temporal::Api::Common::V1::ActivityType, 4
            optional :activity_id, :string, 5
            optional :retry_state, :int32, 6
          end
        end

        struct ChildWorkflowExecutionFailureInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :workflow_type, Temporal::Api::Common::V1::WorkflowType, 3
            optional :initiated_event_id, :int64, 4
            optional :started_event_id, :int64, 5
            optional :retry_state, :int32, 6
          end
        end

        # Failure is self-referential (cause : Failure?) — must use class not struct
        class Failure
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :message, :string, 1
            optional :source, :string, 2
            optional :stack_trace, :string, 3
            optional :encoded_attributes, Temporal::Api::Common::V1::Payload, 20
            optional :cause, Failure, 4
            optional :application_failure_info, ApplicationFailureInfo, 5
            optional :timeout_failure_info, TimeoutFailureInfo, 6
            optional :canceled_failure_info, CanceledFailureInfo, 7
            optional :terminated_failure_info, TerminatedFailureInfo, 8
            optional :server_failure_info, ServerFailureInfo, 9
            optional :reset_workflow_failure_info, ResetWorkflowFailureInfo, 10
            optional :activity_failure_info, ActivityFailureInfo, 11
            optional :child_workflow_execution_failure_info, ChildWorkflowExecutionFailureInfo, 12
          end
        end
      end
    end
  end
end
