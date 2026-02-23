require "protobuf"
require "../google_wkt.pb"

require "./enums.pb"
require "./common.pb"
require "./failure.pb"
require "./update.pb"

# Hand-written Crystal proto bindings for:
#   temporal.api.workflowservice.v1 (request_response.proto)
#   temporal.api.query.v1 (message.proto)
#   temporal.api.workflow.v1 (partial: WorkflowExecutionInfo)
#   temporal.api.history.v1 (partial: HistoryEvent close variants + History)

module Temporal
  module Api
    # ── TaskQueue ────────────────────────────────────────────────────────────
    module Taskqueue
      module V1
        struct TaskQueue
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :name, :string, 1
            optional :kind, :int32, 2
            optional :normal_name, :string, 3
          end
        end
      end
    end

    # ── Query types ──────────────────────────────────────────────────────────
    module Query
      module V1
        struct WorkflowQuery
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :query_type, :string, 1
            optional :query_args, Temporal::Api::Common::V1::Payloads, 2
            optional :header, Temporal::Api::Common::V1::Header, 3
          end
        end

        struct WorkflowQueryResult
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :result_type, :int32, 1
            optional :answer, Temporal::Api::Common::V1::Payloads, 2
            optional :error_message, :string, 3
            optional :failure, Temporal::Api::Failure::V1::Failure, 4
          end
        end

        struct QueryRejected
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :status, :int32, 1
          end
        end
      end
    end

    # ── Workflow execution info ───────────────────────────────────────────────
    module Workflow
      module V1
        struct WorkflowExecutionInfo
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :execution, Temporal::Api::Common::V1::WorkflowExecution, 1
            optional :type, Temporal::Api::Common::V1::WorkflowType, 2
            optional :start_time, Google::Protobuf::Timestamp, 3
            optional :close_time, Google::Protobuf::Timestamp, 4
            optional :status, :int32, 5
            optional :history_length, :int64, 6
            optional :task_queue, :string, 13
            optional :history_size_bytes, :int64, 15
          end
        end
      end
    end

    # ── History types (subset needed for result polling) ─────────────────────
    module History
      module V1
        struct WorkflowExecutionCompletedEventAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :result, Temporal::Api::Common::V1::Payloads, 1
            optional :workflow_task_completed_event_id, :int64, 2
            optional :new_execution_run_id, :string, 3
          end
        end

        struct WorkflowExecutionFailedEventAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :failure, Temporal::Api::Failure::V1::Failure, 1
            optional :retry_state, :int32, 2
            optional :workflow_task_completed_event_id, :int64, 3
            optional :new_execution_run_id, :string, 4
          end
        end

        struct WorkflowExecutionTimedOutEventAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :retry_state, :int32, 1
            optional :new_execution_run_id, :string, 2
          end
        end

        struct WorkflowExecutionCanceledEventAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :workflow_task_completed_event_id, :int64, 1
            optional :details, Temporal::Api::Common::V1::Payloads, 2
          end
        end

        struct WorkflowExecutionTerminatedEventAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :reason, :string, 1
            optional :details, Temporal::Api::Common::V1::Payloads, 2
            optional :identity, :string, 3
          end
        end

        struct WorkflowExecutionContinuedAsNewEventAttributes
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :new_execution_run_id, :string, 1
            optional :workflow_type, Temporal::Api::Common::V1::WorkflowType, 2
            optional :workflow_task_completed_event_id, :int64, 7
            optional :initiator, :int32, 9
          end
        end

        # Minimal HistoryEvent: only the fields needed to follow workflow execution
        struct HistoryEvent
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :event_id, :int64, 1
            optional :event_time, Google::Protobuf::Timestamp, 2
            optional :event_type, :int32, 3
            optional :worker_may_ignore, :bool, 300
            optional :workflow_execution_completed_event_attributes, WorkflowExecutionCompletedEventAttributes, 7
            optional :workflow_execution_failed_event_attributes, WorkflowExecutionFailedEventAttributes, 8
            optional :workflow_execution_timed_out_event_attributes, WorkflowExecutionTimedOutEventAttributes, 9
            optional :workflow_execution_canceled_event_attributes, WorkflowExecutionCanceledEventAttributes, 29
            optional :workflow_execution_terminated_event_attributes, WorkflowExecutionTerminatedEventAttributes, 27
            optional :workflow_execution_continued_as_new_event_attributes, WorkflowExecutionContinuedAsNewEventAttributes, 33
          end
        end

        struct History
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :events, HistoryEvent, 1
          end
        end
      end
    end

    # ── WorkflowService request/response messages ─────────────────────────────
    module Workflowservice
      module V1
        struct StartWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_id, :string, 2
            optional :workflow_type, Temporal::Api::Common::V1::WorkflowType, 3
            optional :task_queue, Temporal::Api::Taskqueue::V1::TaskQueue, 4
            optional :input, Temporal::Api::Common::V1::Payloads, 5
            optional :workflow_execution_timeout, Google::Protobuf::Duration, 6
            optional :workflow_run_timeout, Google::Protobuf::Duration, 7
            optional :workflow_task_timeout, Google::Protobuf::Duration, 8
            optional :identity, :string, 9
            optional :request_id, :string, 10
            optional :workflow_id_reuse_policy, :int32, 11
            optional :workflow_id_conflict_policy, :int32, 22
            optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 12
            optional :cron_schedule, :string, 13
            optional :memo, Temporal::Api::Common::V1::Memo, 14
            optional :search_attributes, Temporal::Api::Common::V1::SearchAttributes, 15
            optional :header, Temporal::Api::Common::V1::Header, 16
            optional :workflow_start_delay, Google::Protobuf::Duration, 20
          end
        end

        struct StartWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :run_id, :string, 1
            optional :started, :bool, 3
            optional :status, :int32, 5
          end
        end

        struct GetWorkflowExecutionHistoryRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :maximum_page_size, :int32, 3
            optional :next_page_token, :bytes, 4
            optional :wait_new_event, :bool, 5
            optional :history_event_filter_type, :int32, 6
            optional :skip_archival, :bool, 7
          end
        end

        struct GetWorkflowExecutionHistoryResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :history, Temporal::Api::History::V1::History, 1
            optional :next_page_token, :bytes, 3
            optional :archived, :bool, 4
          end
        end

        struct RequestCancelWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :identity, :string, 3
            optional :request_id, :string, 4
            optional :first_execution_run_id, :string, 5
            optional :reason, :string, 6
          end
        end

        struct RequestCancelWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
          end
        end

        struct SignalWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :signal_name, :string, 3
            optional :input, Temporal::Api::Common::V1::Payloads, 4
            optional :identity, :string, 5
            optional :request_id, :string, 6
            optional :header, Temporal::Api::Common::V1::Header, 8
          end
        end

        struct SignalWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
          end
        end

        struct SignalWithStartWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_id, :string, 2
            optional :workflow_type, Temporal::Api::Common::V1::WorkflowType, 3
            optional :task_queue, Temporal::Api::Taskqueue::V1::TaskQueue, 4
            optional :input, Temporal::Api::Common::V1::Payloads, 5
            optional :workflow_execution_timeout, Google::Protobuf::Duration, 6
            optional :workflow_run_timeout, Google::Protobuf::Duration, 7
            optional :workflow_task_timeout, Google::Protobuf::Duration, 8
            optional :identity, :string, 9
            optional :request_id, :string, 10
            optional :workflow_id_reuse_policy, :int32, 11
            optional :workflow_id_conflict_policy, :int32, 22
            optional :signal_name, :string, 12
            optional :signal_input, Temporal::Api::Common::V1::Payloads, 14
            optional :retry_policy, Temporal::Api::Common::V1::RetryPolicy, 17
            optional :cron_schedule, :string, 18
            optional :memo, Temporal::Api::Common::V1::Memo, 19
            optional :search_attributes, Temporal::Api::Common::V1::SearchAttributes, 20
            optional :header, Temporal::Api::Common::V1::Header, 21
          end
        end

        struct SignalWithStartWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :run_id, :string, 1
            optional :started, :bool, 2
          end
        end

        struct TerminateWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :reason, :string, 3
            optional :details, Temporal::Api::Common::V1::Payloads, 4
            optional :identity, :string, 5
            optional :first_execution_run_id, :string, 6
          end
        end

        struct TerminateWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
          end
        end

        struct QueryWorkflowRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :query, Temporal::Api::Query::V1::WorkflowQuery, 3
            optional :query_reject_condition, :int32, 4
          end
        end

        struct QueryWorkflowResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :query_result, Temporal::Api::Common::V1::Payloads, 1
            optional :query_rejected, Temporal::Api::Query::V1::QueryRejected, 2
          end
        end

        struct DescribeWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :execution, Temporal::Api::Common::V1::WorkflowExecution, 2
          end
        end

        struct DescribeWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :workflow_execution_info, Temporal::Api::Workflow::V1::WorkflowExecutionInfo, 2
          end
        end

        struct ListWorkflowExecutionsRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :page_size, :int32, 2
            optional :next_page_token, :bytes, 3
            optional :query, :string, 4
          end
        end

        struct ListWorkflowExecutionsResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            repeated :executions, Temporal::Api::Workflow::V1::WorkflowExecutionInfo, 1
            optional :next_page_token, :bytes, 2
          end
        end

        struct CountWorkflowExecutionsRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :query, :string, 2
          end
        end

        struct CountWorkflowExecutionsResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :count, :int64, 1
          end
        end

        struct UpdateWorkflowExecutionRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :workflow_execution, Temporal::Api::Common::V1::WorkflowExecution, 2
            optional :first_execution_run_id, :string, 3
            optional :wait_policy, Temporal::Api::Update::V1::WaitPolicy, 4
            optional :request, Temporal::Api::Update::V1::Request, 5
          end
        end

        struct UpdateWorkflowExecutionResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :update_ref, Temporal::Api::Update::V1::UpdateRef, 1
            optional :outcome, Temporal::Api::Update::V1::Outcome, 2
            optional :stage, :int32, 3
          end
        end

        struct PollWorkflowExecutionUpdateRequest
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :namespace, :string, 1
            optional :update_ref, Temporal::Api::Update::V1::UpdateRef, 2
            optional :identity, :string, 3
            optional :wait_policy, Temporal::Api::Update::V1::WaitPolicy, 4
          end
        end

        struct PollWorkflowExecutionUpdateResponse
          include ::Protobuf::Message
          contract_of "proto3" do
            optional :outcome, Temporal::Api::Update::V1::Outcome, 1
            optional :stage, :int32, 2
          end
        end
      end
    end
  end
end
