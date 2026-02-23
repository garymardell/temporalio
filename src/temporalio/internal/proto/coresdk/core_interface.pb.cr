require "protobuf"

require "../api/common.pb"
require "./activity_result.pb"

# Hand-written Crystal proto bindings for coresdk (core_interface.proto)

module Coresdk
  struct ActivityHeartbeat
    include ::Protobuf::Message
    contract_of "proto3" do
      optional :task_token, :bytes, 1
      repeated :details, Temporal::Api::Common::V1::Payload, 2
    end
  end

  struct ActivityTaskCompletion
    include ::Protobuf::Message
    contract_of "proto3" do
      optional :task_token, :bytes, 1
      optional :result, Coresdk::ActivityResult::ActivityExecutionResult, 2
    end
  end
end
