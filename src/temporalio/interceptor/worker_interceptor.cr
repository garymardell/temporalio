require "../internal/proto"
require "../data_converter"

module Temporalio
  module Interceptor
    # Input types for worker/activity interceptor methods.

    struct ExecuteActivityInput
      getter activity_type : String
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter task_token : Bytes

      def initialize(@activity_type : String, @args : Array(Temporal::Api::Common::V1::Payload), @task_token : Bytes)
      end
    end

    struct ScheduleActivityInput
      getter activity_type : String
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter task_queue : String?
      getter schedule_to_close_timeout : Time::Span?
      getter start_to_close_timeout : Time::Span?
      getter heartbeat_timeout : Time::Span?

      def initialize(
        @activity_type : String,
        @args : Array(Temporal::Api::Common::V1::Payload),
        @task_queue : String? = nil,
        @schedule_to_close_timeout : Time::Span? = nil,
        @start_to_close_timeout : Time::Span? = nil,
        @heartbeat_timeout : Time::Span? = nil
      )
      end
    end

    struct StartTimerInput
      getter duration : Time::Span

      def initialize(@duration : Time::Span)
      end
    end

    struct ExecuteChildWorkflowInput
      getter workflow_type : String
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter workflow_id : String?
      getter task_queue : String?

      def initialize(
        @workflow_type : String,
        @args : Array(Temporal::Api::Common::V1::Payload),
        @workflow_id : String? = nil,
        @task_queue : String? = nil
      )
      end
    end

    struct SignalExternalWorkflowInput
      getter workflow_id : String
      getter signal_name : String
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter run_id : String?
      getter namespace : String?

      def initialize(
        @workflow_id : String,
        @signal_name : String,
        @args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
        @run_id : String? = nil,
        @namespace : String? = nil
      )
      end
    end

    struct CancelExternalWorkflowInput
      getter workflow_id : String
      getter run_id : String?
      getter namespace : String?
      getter reason : String?

      def initialize(
        @workflow_id : String,
        @run_id : String? = nil,
        @namespace : String? = nil,
        @reason : String? = nil
      )
      end
    end

    struct ContinueAsNewInput
      getter workflow_type : String?
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter task_queue : String?

      def initialize(
        @workflow_type : String? = nil,
        @args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
        @task_queue : String? = nil
      )
      end
    end

    struct HandleSignalInput
      getter signal_name : String
      getter args : Array(Temporal::Api::Common::V1::Payload)

      def initialize(
        @signal_name : String,
        @args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload
      )
      end
    end

    struct HandleQueryInput
      getter query_id : String
      getter query_type : String
      getter args : Array(Temporal::Api::Common::V1::Payload)

      def initialize(
        @query_id : String,
        @query_type : String,
        @args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload
      )
      end
    end

    struct HandleUpdateInput
      getter update_id : String
      getter update_name : String
      getter args : Array(Temporal::Api::Common::V1::Payload)

      def initialize(
        @update_id : String,
        @update_name : String,
        @args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload
      )
      end
    end

    # Base class for worker-side interceptors (activity inbound + workflow outbound).
    # Subclass and override only the methods you need; defaults pass through.
    class WorkerInterceptor
      # Inbound: wraps execution of an activity.
      def execute_activity(
        input : ExecuteActivityInput,
        next_fn : Proc(ExecuteActivityInput, Temporal::Api::Common::V1::Payload?)
      ) : Temporal::Api::Common::V1::Payload?
        next_fn.call(input)
      end

      # Outbound: wraps scheduling an activity from within a workflow.
      def schedule_activity(
        input : ScheduleActivityInput,
        next_fn : Proc(ScheduleActivityInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Outbound: wraps starting a timer from within a workflow.
      def start_timer(
        input : StartTimerInput,
        next_fn : Proc(StartTimerInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Outbound: wraps executing a child workflow from within a workflow.
      def execute_child_workflow(
        input : ExecuteChildWorkflowInput,
        next_fn : Proc(ExecuteChildWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Outbound: wraps signalling an external workflow from within a workflow.
      def signal_external_workflow(
        input : SignalExternalWorkflowInput,
        next_fn : Proc(SignalExternalWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Outbound: wraps requesting cancellation of an external workflow.
      def cancel_external_workflow(
        input : CancelExternalWorkflowInput,
        next_fn : Proc(CancelExternalWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Outbound: wraps continue-as-new from within a workflow.
      def continue_as_new(
        input : ContinueAsNewInput,
        next_fn : Proc(ContinueAsNewInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Inbound: wraps handling a signal in a workflow.
      def handle_signal(
        input : HandleSignalInput,
        next_fn : Proc(HandleSignalInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      # Inbound: wraps handling a query in a workflow.
      def handle_query(
        input : HandleQueryInput,
        next_fn : Proc(HandleQueryInput, Temporal::Api::Common::V1::Payload?)
      ) : Temporal::Api::Common::V1::Payload?
        next_fn.call(input)
      end

      # Inbound: wraps handling an update in a workflow.
      def handle_update(
        input : HandleUpdateInput,
        next_fn : Proc(HandleUpdateInput, Temporal::Api::Common::V1::Payload?)
      ) : Temporal::Api::Common::V1::Payload?
        next_fn.call(input)
      end
    end
  end
end
