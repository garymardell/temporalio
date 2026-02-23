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
    end
  end
end
