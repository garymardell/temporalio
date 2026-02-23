require "protobuf"

require "../api/common.pb"
require "../api/failure.pb"

# Hand-written Crystal proto bindings for coresdk.child_workflow

module Coresdk
  module ChildWorkflow
    enum ParentClosePolicy
      PARENT_CLOSE_POLICY_UNSPECIFIED    = 0
      PARENT_CLOSE_POLICY_TERMINATE      = 1
      PARENT_CLOSE_POLICY_ABANDON        = 2
      PARENT_CLOSE_POLICY_REQUEST_CANCEL = 3
    end

    enum StartChildWorkflowExecutionFailedCause
      START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED           = 0
      START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS = 1
    end

    enum ChildWorkflowCancellationType
      ABANDON                    = 0
      TRY_CANCEL                 = 1
      WAIT_CANCELLATION_COMPLETED = 2
      WAIT_CANCELLATION_REQUESTED = 3
    end

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

    # oneof status
    struct ChildWorkflowResult
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :completed, Success, 1
        optional :failed, Failure, 2
        optional :cancelled, Cancellation, 3
      end
    end
  end
end
