require "protobuf"
require "../google_wkt.pb"

require "../api/common.pb"
require "../api/failure.pb"
require "./common.pb"
require "./child_workflow.pb"

# Hand-written Crystal proto bindings for coresdk.workflow_commands

module Coresdk
  module WorkflowCommands
    enum ActivityCancellationType
      TRY_CANCEL                 = 0
      WAIT_CANCELLATION_COMPLETED = 1
      ABANDON                    = 2
    end

    # Map entry helpers
    struct StringPayloadEntry
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :key, :string, 1
        optional :value, Temporal::Api::Common::V1::Payload, 2
      end
    end

    struct StartTimer
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :start_to_fire_timeout, Google::Protobuf::Duration, 2
      end
    end

    struct CancelTimer
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
      end
    end

    struct ScheduleActivity
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :activity_id, :string, 2
        optional :activity_type, :string, 3
        optional :task_queue, :string, 5
        repeated :headers, StringPayloadEntry, 6
        repeated :arguments, Temporal::Api::Common::V1::Payload, 7
        optional :schedule_to_close_timeout, Google::Protobuf::Duration, 8
        optional :schedule_to_start_timeout, Google::Protobuf::Duration, 9
        optional :start_to_close_timeout, Google::Protobuf::Duration, 10
        optional :heartbeat_timeout, Google::Protobuf::Duration, 11
        optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 12
        optional :cancellation_type, :int32, 13
        optional :do_not_eagerly_execute, :bool, 14
        optional :versioning_intent, :int32, 15
      end
    end

    struct ScheduleLocalActivity
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :activity_id, :string, 2
        optional :activity_type, :string, 3
        optional :attempt, :uint32, 4
        optional :original_schedule_time, Google::Protobuf::Timestamp, 5
        repeated :headers, StringPayloadEntry, 6
        repeated :arguments, Temporal::Api::Common::V1::Payload, 7
        optional :schedule_to_close_timeout, Google::Protobuf::Duration, 8
        optional :schedule_to_start_timeout, Google::Protobuf::Duration, 9
        optional :start_to_close_timeout, Google::Protobuf::Duration, 10
        optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 11
        optional :local_retry_threshold, Google::Protobuf::Duration, 12
        optional :cancellation_type, :int32, 13
      end
    end

    struct RequestCancelActivity
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
      end
    end

    struct RequestCancelLocalActivity
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
      end
    end

    struct QuerySuccess
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :response, Temporal::Api::Common::V1::Payload, 1
      end
    end

    # oneof variant
    struct QueryResult
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :query_id, :string, 1
        optional :succeeded, QuerySuccess, 2
        optional :failed, Temporal::Api::Failure::V1::Failure, 3
      end
    end

    struct CompleteWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :result, Temporal::Api::Common::V1::Payload, 1
      end
    end

    struct FailWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :failure, Temporal::Api::Failure::V1::Failure, 1
      end
    end

    struct ContinueAsNewWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :workflow_type, :string, 1
        optional :task_queue, :string, 2
        repeated :arguments, Temporal::Api::Common::V1::Payload, 3
        optional :workflow_run_timeout, Google::Protobuf::Duration, 4
        optional :workflow_task_timeout, Google::Protobuf::Duration, 5
        repeated :memo, StringPayloadEntry, 6
        repeated :headers, StringPayloadEntry, 7
        repeated :search_attributes, StringPayloadEntry, 8
        optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 9
        optional :versioning_intent, :int32, 10
      end
    end

    struct CancelWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
      end
    end

    struct SetPatchMarker
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :patch_id, :string, 1
        optional :deprecated, :bool, 2
      end
    end

    struct StartChildWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :namespace, :string, 2
        optional :workflow_id, :string, 3
        optional :workflow_type, :string, 4
        optional :task_queue, :string, 5
        repeated :input, Temporal::Api::Common::V1::Payload, 6
        optional :workflow_execution_timeout, Google::Protobuf::Duration, 7
        optional :workflow_run_timeout, Google::Protobuf::Duration, 8
        optional :workflow_task_timeout, Google::Protobuf::Duration, 9
        optional :parent_close_policy, :int32, 10
        optional :workflow_id_reuse_policy, :int32, 12
        optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 13
        optional :cron_schedule, :string, 14
        repeated :headers, StringPayloadEntry, 15
        repeated :memo, StringPayloadEntry, 16
        repeated :search_attributes, StringPayloadEntry, 17
        optional :cancellation_type, :int32, 18
        optional :versioning_intent, :int32, 19
      end
    end

    struct CancelChildWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :child_workflow_seq, :uint32, 1
        optional :reason, :string, 2
      end
    end

    struct RequestCancelExternalWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :workflow_execution, Coresdk::Common::NamespacedWorkflowExecution, 2
        optional :reason, :string, 3
      end
    end

    struct SignalExternalWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
        optional :workflow_execution, Coresdk::Common::NamespacedWorkflowExecution, 2
        optional :child_workflow_id, :string, 3
        optional :signal_name, :string, 4
        repeated :args, Temporal::Api::Common::V1::Payload, 5
        repeated :headers, StringPayloadEntry, 6
      end
    end

    struct CancelSignalWorkflow
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seq, :uint32, 1
      end
    end

    struct UpsertWorkflowSearchAttributes
      include ::Protobuf::Message
      contract_of "proto3" do
        repeated :search_attributes, StringPayloadEntry, 1
      end
    end

    struct ModifyWorkflowProperties
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :upserted_memo, Temporal::Api::Common::V1::Memo, 1
      end
    end

    struct UpdateResponse
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :protocol_instance_id, :string, 1
        optional :accepted, Google::Protobuf::Empty, 2
        optional :rejected, Temporal::Api::Failure::V1::Failure, 3
        optional :completed, Temporal::Api::Common::V1::Payload, 4
      end
    end

    # oneof variant — one of these will be set per WorkflowCommand
    struct WorkflowCommand
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :start_timer, StartTimer, 1
        optional :schedule_activity, ScheduleActivity, 2
        optional :respond_to_query, QueryResult, 3
        optional :request_cancel_activity, RequestCancelActivity, 4
        optional :cancel_timer, CancelTimer, 5
        optional :complete_workflow_execution, CompleteWorkflowExecution, 6
        optional :fail_workflow_execution, FailWorkflowExecution, 7
        optional :continue_as_new_workflow_execution, ContinueAsNewWorkflowExecution, 8
        optional :cancel_workflow_execution, CancelWorkflowExecution, 9
        optional :set_patch_marker, SetPatchMarker, 10
        optional :start_child_workflow_execution, StartChildWorkflowExecution, 11
        optional :cancel_child_workflow_execution, CancelChildWorkflowExecution, 12
        optional :request_cancel_external_workflow_execution, RequestCancelExternalWorkflowExecution, 13
        optional :signal_external_workflow_execution, SignalExternalWorkflowExecution, 14
        optional :cancel_signal_workflow, CancelSignalWorkflow, 15
        optional :schedule_local_activity, ScheduleLocalActivity, 16
        optional :request_cancel_local_activity, RequestCancelLocalActivity, 17
        optional :upsert_workflow_search_attributes, UpsertWorkflowSearchAttributes, 18
        optional :modify_workflow_properties, ModifyWorkflowProperties, 19
        optional :update_response, UpdateResponse, 20
      end
    end
  end
end
