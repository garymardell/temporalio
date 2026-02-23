require "protobuf"
require "../google_wkt.pb"

# Hand-written Crystal proto bindings for temporal.api.common.v1.message.proto
# Uses struct + include ::Protobuf::Message + contract_of "proto3" (correct protobuf.cr v2 style)
# Maps are encoded as repeated MapFieldEntry messages (key=1, value=2)

module Temporal
  module Api
    module Common
      module V1
        # Map entry helper for map<string, bytes> (e.g. Payload.metadata)
        struct StringBytesEntry
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :key, :string, 1
            optional :value, :bytes, 2
          end
        end

        # Map entry helper for map<string, Payload>
        struct StringPayloadEntry
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :key, :string, 1
            optional :value, Payload, 2
          end
        end

        struct Payload
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :metadata, StringBytesEntry, 1
            optional :data, :bytes, 2
          end
        end

        struct Payloads
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :payloads, Payload, 1
          end
        end

        struct SearchAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :indexed_fields, StringPayloadEntry, 1
          end
        end

        struct MemoFieldsEntry
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :key, :string, 1
            optional :value, Payload, 2
          end
        end

        struct Memo
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :fields, MemoFieldsEntry, 1
          end
        end

        struct HeaderFieldsEntry
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :key, :string, 1
            optional :value, Payload, 2
          end
        end

        struct Header
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :fields, HeaderFieldsEntry, 1
          end
        end

        struct WorkflowExecution
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :workflow_id, :string, 1
            optional :run_id, :string, 2
          end
        end

        struct WorkflowType
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :name, :string, 1
          end
        end

        struct ActivityType
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :name, :string, 1
          end
        end

        struct RetryPolicy
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :initial_interval, Google::Protobuf::Duration, 1
            optional :backoff_coefficient, :double, 2
            optional :maximum_interval, Google::Protobuf::Duration, 3
            optional :maximum_attempts, :int32, 4
            repeated :non_retryable_error_types, :string, 5
          end
        end

        struct DataBlob
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :encoding_type, :int32, 1
            optional :data, :bytes, 2
          end
        end

        struct Priority
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :priority_key, :int32, 1
          end
        end
      end
    end
  end
end
