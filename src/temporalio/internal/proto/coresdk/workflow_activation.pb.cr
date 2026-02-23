require "protobuf"
require "../google_wkt.pb"

require "../api/common.pb"
require "../api/failure.pb"
require "./common.pb"
require "./activity_result.pb"
require "./child_workflow.pb"

# Hand-written Crystal proto bindings for coresdk.workflow_activation

module Coresdk
  module WorkflowActivation
    # Map entry helpers
    struct StringPayloadEntry
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :key, :string, 1
        optional :value, Temporal::Api::Common::V1::Payload, 2
      end
    end

    struct InitializeWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :workflow_type, :string, 1
        optional :workflow_id, :string, 2
        repeated :arguments, Temporal::Api::Common::V1::Payload, 3
        optional :randomness_seed, :uint64, 4
        repeated :headers, StringPayloadEntry, 5
        optional :identity, :string, 6
        optional :parent_workflow_info, Coresdk::Common::NamespacedWorkflowExecution, 7
        optional :workflow_execution_timeout, Google::Protobuf::Duration, 8
        optional :workflow_run_timeout, Google::Protobuf::Duration, 9
        optional :workflow_task_timeout, Google::Protobuf::Duration, 10
        optional :continued_from_execution_run_id, :string, 11
        optional :continued_initiator, :int32, 12
        optional :continued_failure, Temporal::Api::Failure::V1::Failure, 13
        optional :last_completion_result, Temporal::Api::Common::V1::Payloads, 14
        optional :first_execution_run_id, :string, 15
        optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 16
        optional :attempt, :int32, 17
        optional :cron_schedule, :string, 18
        optional :workflow_execution_expiration_time, Google::Protobuf::Timestamp, 19
        optional :cron_schedule_to_schedule_interval, Google::Protobuf::Duration, 20
        optional :memo, Temporal::Api::Common::V1::Memo, 21
        optional :search_attributes, Temporal::Api::Common::V1::SearchAttributes, 22
        optional :start_time, Google::Protobuf::Timestamp, 23
        optional :root_workflow, Temporal::Api::Common::V1::WorkflowExecution, 24
      end
    end

    struct FireTimer
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
      end
    end

    struct ResolveActivity
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :result, Coresdk::ActivityResult::ActivityResolution, 2
        optional :is_local, :bool, 3
      end
    end

    struct ResolveChildWorkflowExecutionStartSuccess
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :run_id, :string, 1
      end
    end

    struct ResolveChildWorkflowExecutionStartFailure
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :workflow_id, :string, 1
        optional :workflow_type, :string, 2
        optional :cause, :int32, 3
      end
    end

    struct ResolveChildWorkflowExecutionStartCancelled
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :failure, Temporal::Api::Failure::V1::Failure, 1
      end
    end

    # oneof status
    struct ResolveChildWorkflowExecutionStart
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :succeeded, ResolveChildWorkflowExecutionStartSuccess, 2
        optional :failed, ResolveChildWorkflowExecutionStartFailure, 3
        optional :cancelled, ResolveChildWorkflowExecutionStartCancelled, 4
      end
    end

    struct ResolveChildWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :result, Coresdk::ChildWorkflow::ChildWorkflowResult, 2
      end
    end

    struct UpdateRandomSeed
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :randomness_seed, :uint64, 1
      end
    end

    struct QueryWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :query_id, :string, 1
        optional :query_type, :string, 2
        repeated :arguments, Temporal::Api::Common::V1::Payload, 3
        repeated :headers, StringPayloadEntry, 5
      end
    end

    struct CancelWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :reason, :string, 1
      end
    end

    struct SignalWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :signal_name, :string, 1
        repeated :input, Temporal::Api::Common::V1::Payload, 2
        optional :identity, :string, 3
        repeated :headers, StringPayloadEntry, 5
      end
    end

    struct NotifyHasPatch
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :patch_id, :string, 1
      end
    end

    struct ResolveSignalExternalWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :failure, Temporal::Api::Failure::V1::Failure, 2
      end
    end

    struct ResolveRequestCancelExternalWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :failure, Temporal::Api::Failure::V1::Failure, 2
      end
    end

    struct DoUpdate
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :id, :string, 1
        optional :protocol_instance_id, :string, 2
        optional :name, :string, 3
        repeated :input, Temporal::Api::Common::V1::Payload, 4
        repeated :headers, StringPayloadEntry, 5
        optional :run_validator, :bool, 7
      end
    end

    struct RemoveFromCache
      include ::Protobuf::Message

      enum EvictionReason
        UNSPECIFIED                 = 0
        CACHE_FULL                  = 1
        CACHE_MISS                  = 2
        NONDETERMINISM              = 3
        LANG_FAIL                   = 4
        LANG_REQUESTED              = 5
        TASK_NOT_FOUND              = 6
        UNHANDLED_COMMAND           = 7
        FATAL                       = 8
        PAGINATION_OR_HISTORY_FETCH = 9
        WORKFLOW_EXECUTION_ENDING   = 10
      end

      contract_of "proto3" do
        optional :message, :string, 1
        optional :reason, :int32, 2
      end
    end

    # oneof variant — represented as optional fields (only one will be set per activation job)
    struct WorkflowActivationJob
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :initialize_workflow, InitializeWorkflow, 1
        optional :fire_timer, FireTimer, 2
        optional :update_random_seed, UpdateRandomSeed, 4
        optional :query_workflow, QueryWorkflow, 5
        optional :cancel_workflow, CancelWorkflow, 6
        optional :signal_workflow, SignalWorkflow, 7
        optional :resolve_activity, ResolveActivity, 8
        optional :notify_has_patch, NotifyHasPatch, 9
        optional :resolve_child_workflow_execution_start, ResolveChildWorkflowExecutionStart, 10
        optional :resolve_child_workflow_execution, ResolveChildWorkflowExecution, 11
        optional :resolve_signal_external_workflow, ResolveSignalExternalWorkflow, 12
        optional :resolve_request_cancel_external_workflow, ResolveRequestCancelExternalWorkflow, 13
        optional :do_update, DoUpdate, 14
        optional :remove_from_cache, RemoveFromCache, 50
      end
    end

    struct WorkflowActivation
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :run_id, :string, 1
        optional :timestamp, Google::Protobuf::Timestamp, 2
        optional :is_replaying, :bool, 3
        optional :history_length, :uint32, 4
        repeated :jobs, WorkflowActivationJob, 5
        repeated :available_internal_flags, :uint32, 6
        optional :history_size_bytes, :uint64, 7
        optional :continue_as_new_suggested, :bool, 8
        optional :deployment_version_for_current_task, Coresdk::Common::WorkerDeploymentVersion, 9
        optional :last_sdk_version, :string, 10
      end
    end
  end
end
