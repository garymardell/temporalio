require "protobuf"
require "../google_wkt.pb"

require "../api/common.pb"
require "../api/failure.pb"

# Hand-written Crystal proto bindings for coresdk.activity_result

module Coresdk
  module ActivityResult
    struct Success
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :result, Temporal::Api::Common::V1::Payload, 1
      end
    end

    struct Failure
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :failure, Temporal::Api::Failure::V1::Failure, 1
      end
    end

    struct Cancellation
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :failure, Temporal::Api::Failure::V1::Failure, 1
      end
    end

    struct WillCompleteAsync
      include ::Protobuf::Message
      contract_of "proto3" do
      end
    end

    struct DoBackoff
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :attempt, :uint32, 1
        optional :backoff_duration, Google::Protobuf::Duration, 2
        optional :original_schedule_time, Google::Protobuf::Timestamp, 3
      end
    end

    # oneof status — represented as optional fields (only one will be set)
    struct ActivityExecutionResult
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :completed, Success, 1
        optional :failed, Failure, 2
        optional :cancelled, Cancellation, 3
        optional :will_complete_async, WillCompleteAsync, 4
      end
    end

    struct ActivityResolution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :completed, Success, 1
        optional :failed, Failure, 2
        optional :cancelled, Cancellation, 3
        optional :backoff, DoBackoff, 4
      end
    end
  end
end
