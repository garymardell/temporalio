require "protobuf"

require "../api/failure.pb"
require "../api/enums.pb"
require "./workflow_commands.pb"

# Hand-written Crystal proto bindings for coresdk.workflow_completion

module Coresdk
  module WorkflowCompletion
    struct Success
      include ::Protobuf::Message
      contract_of "proto3" do
        repeated :commands, Coresdk::WorkflowCommands::WorkflowCommand, 1
        repeated :used_internal_flags, :uint32, 6
        optional :versioning_behavior, :int32, 7
      end
    end

    struct Failure
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :failure, Temporal::Api::Failure::V1::Failure, 1
        optional :force_cause, :int32, 2
      end
    end

    # oneof status
    struct WorkflowActivationCompletion
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :run_id, :string, 1
        optional :successful, Success, 2
        optional :failed, Failure, 3
      end
    end
  end
end
