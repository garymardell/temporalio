require "protobuf"
require "../google_wkt.pb"

require "../api/common.pb"

# Hand-written Crystal proto bindings for coresdk.activity_task

module Coresdk
  module ActivityTask
    enum ActivityCancelReason
      NOT_FOUND       = 0
      CANCELLED       = 1
      TIMED_OUT       = 2
      WORKER_SHUTDOWN = 3
      PAUSED          = 4
      RESET           = 5
    end

    # map<string, Payload> header_fields — represented as repeated entry
    struct HeaderFieldsEntry
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :key, :string, 1
        optional :value, Temporal::Api::Common::V1::Payload, 2
      end
    end

    struct ActivityCancellationDetails
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :is_not_found, :bool, 1
        optional :is_cancelled, :bool, 2
        optional :is_paused, :bool, 3
        optional :is_timed_out, :bool, 4
        optional :is_worker_shutdown, :bool, 5
        optional :is_reset, :bool, 6
      end
    end

    struct Start
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :workflow_namespace, :string, 1
        optional :workflow_type, :string, 2
        optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 3
        optional :activity_id, :string, 4
        optional :activity_type, :string, 5
        repeated :header_fields, HeaderFieldsEntry, 6
        repeated :input, Temporal::Api::Common::V1::Payload, 7
        repeated :heartbeat_details, Temporal::Api::Common::V1::Payload, 8
        optional :scheduled_time, Google::Protobuf::Timestamp, 9
        optional :current_attempt_scheduled_time, Google::Protobuf::Timestamp, 10
        optional :started_time, Google::Protobuf::Timestamp, 11
        optional :attempt, :uint32, 12
        optional :schedule_to_close_timeout, Google::Protobuf::Duration, 13
        optional :start_to_close_timeout, Google::Protobuf::Duration, 14
        optional :heartbeat_timeout, Google::Protobuf::Duration, 15
        optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 16
        optional :is_local, :bool, 17
      end
    end

    struct Cancel
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :reason, :int32, 1
        optional :details, ActivityCancellationDetails, 2
      end
    end

    # oneof variant — represented as optional fields
    struct ActivityTask
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :task_token, :bytes, 1
        optional :start, Start, 3
        optional :cancel, Cancel, 4
      end
    end
  end
end
