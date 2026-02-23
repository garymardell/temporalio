require "protobuf"

module Temporal
  module Api
    module Update
      module V1
        # Specifies client's intent to wait for Update results
        struct WaitPolicy
          include Protobuf::Message
          contract_of "proto3" do
            optional :lifecycle_stage, :int32, 1
          end
        end

        # The data needed by a client to refer to a previously invoked Workflow Update
        struct UpdateRef
          include Protobuf::Message
          contract_of "proto3" do
            optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 1
            optional :update_id, :string, 2
          end
        end

        # The outcome of a Workflow Update - success or failure
        struct Outcome
          include Protobuf::Message
          contract_of "proto3" do
            optional :success, Temporal::Api::Common::V1::Payloads, 1
            optional :failure, Temporal::Api::Failure::V1::Failure, 2
          end
        end

        # Metadata about a Workflow Update
        struct Meta
          include Protobuf::Message
          contract_of "proto3" do
            optional :update_id, :string, 1
            optional :identity, :string, 2
          end
        end

        # Input for an Update
        struct Input
          include Protobuf::Message
          contract_of "proto3" do
            optional :header, Temporal::Api::Common::V1::Header, 1
            optional :name, :string, 2
            optional :args, Temporal::Api::Common::V1::Payloads, 3
          end
        end

        # The client request that triggers a Workflow Update
        struct Request
          include Protobuf::Message
          contract_of "proto3" do
            optional :meta, Meta, 1
            optional :input, Input, 2
          end
        end
      end
    end
  end
end
