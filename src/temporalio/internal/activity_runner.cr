require "../activity/context"
require "../activity/activity_info"
require "../data_converter"
require "../internal/failure_converter"
require "../internal/proto"
require "../interceptor/worker_interceptor"

module Temporalio
  module Internal
    # Handles execution of a single activity task.
    class ActivityRunner
      def initialize(
        @bridge_worker : Bridge::Worker,
        @data_converter : DataConverter,
        @task : Coresdk::ActivityTask::ActivityTask,
        @heartbeat_semaphore : Channel(Nil) = Channel(Nil).new(1),
        @interceptors : Array(Temporalio::Interceptor::WorkerInterceptor) = [] of Temporalio::Interceptor::WorkerInterceptor
      )
      end

      # Execute the activity in the current fiber.
      # Calls the activity class's execute method and sends a completion.
      def run(activity_class : ActivityDefinition) : Nil
        task_token = @task.task_token || Bytes.empty
        start = @task.start

        unless start
          # Cancel task — handled separately (cancel fires on the activity context)
          return
        end

        # Build info
        info = Activity::ActivityInfo.from_proto(
          task_token,
          start,
          @data_converter
        )

        # Build heartbeat proc
        heartbeat_proc = Proc(Array(Temporal::Api::Common::V1::Payload), Nil).new do |detail_payloads|
          send_heartbeat(task_token, detail_payloads)
        end

        context = Activity::Context.new(info, heartbeat_proc)
        context.install!

        result_payload : Temporal::Api::Common::V1::Payload? = nil
        failure : Temporal::Api::Failure::V1::Failure? = nil

        begin
          input_payloads = start.input || [] of Temporal::Api::Common::V1::Payload

          # Thread execute_activity through interceptors
          intercept_input = Temporalio::Interceptor::ExecuteActivityInput.new(
            activity_type: info.activity_type,
            args: input_payloads,
            task_token: task_token
          )
          chain = @interceptors.reverse
          inner_fn = Proc(Temporalio::Interceptor::ExecuteActivityInput, Temporal::Api::Common::V1::Payload?).new do |inp|
            activity_class.execute_activity(inp.args, @data_converter).as(Temporal::Api::Common::V1::Payload?)
          end
          fn = chain.reduce(inner_fn) do |next_fn, interceptor|
            Proc(Temporalio::Interceptor::ExecuteActivityInput, Temporal::Api::Common::V1::Payload?).new do |i|
              interceptor.execute_activity(i, next_fn)
            end
          end
          result_payload = fn.call(intercept_input)
        rescue ex : CancelledError
          failure = FailureConverter.to_failure(ex, @data_converter)
        rescue ex : Exception
          failure = FailureConverter.to_failure(ex, @data_converter)
        ensure
          context.uninstall!
        end

        # Send completion
        exec_result = if f = failure
          Coresdk::ActivityResult::ActivityExecutionResult.new(
            failed: Coresdk::ActivityResult::Failure.new(failure: f)
          )
        elsif p = result_payload
          Coresdk::ActivityResult::ActivityExecutionResult.new(
            completed: Coresdk::ActivityResult::Success.new(result: p)
          )
        else
          Coresdk::ActivityResult::ActivityExecutionResult.new(
            completed: Coresdk::ActivityResult::Success.new(result: nil)
          )
        end

        completion = Coresdk::ActivityTaskCompletion.new(
          task_token: task_token,
          result: exec_result
        )

        @bridge_worker.complete_activity_task(completion.to_protobuf.to_slice)
      end

      private def send_heartbeat(task_token : Bytes, detail_payloads : Array(Temporal::Api::Common::V1::Payload)) : Nil
        heartbeat = Coresdk::ActivityHeartbeat.new(
          task_token: task_token,
          details: detail_payloads
        )
        error = @bridge_worker.record_activity_heartbeat(heartbeat.to_protobuf.to_slice)
        # If error is non-nil, it's a cancellation signal — ActivityContext already tracks this
      end
    end

    # Describes a registered activity class.
    # Decouples the worker from having to know the concrete type at compile time.
    abstract class ActivityDefinition
      abstract def activity_name : String
      abstract def execute_activity(
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?
    end

    # Concrete definition for a class that includes Temporalio::Activity.
    class ConcreteActivityDefinition(T) < ActivityDefinition
      def activity_name : String
        T.activity_name
      end

      def execute_activity(
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?
        instance = T.new
        instance._temporal_execute(payloads, converter)
      end
    end
  end
end
