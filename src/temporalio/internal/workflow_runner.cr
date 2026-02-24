require "../internal/proto"
require "../internal/workflow_instance"
require "../internal/failure_converter"
require "../data_converter"
require "../interceptor/worker_interceptor"

module Temporalio
  module Internal
    # Type-erased factory that creates WorkflowInstance for a given type T.
    abstract class WorkflowDefinition
      abstract def workflow_name : String
      abstract def new_instance(
        activation : Coresdk::WorkflowActivation::WorkflowActivation,
        data_converter : DataConverter,
        namespace : String,
        task_queue : String,
        interceptors : Array(Temporalio::Interceptor::WorkerInterceptor)
      ) : WorkflowInstance
    end

    class ConcreteWorkflowDefinition(T) < WorkflowDefinition
      def workflow_name : String
        T.workflow_name
      end

      def new_instance(
        activation : Coresdk::WorkflowActivation::WorkflowActivation,
        data_converter : DataConverter,
        namespace : String,
        task_queue : String,
        interceptors : Array(Temporalio::Interceptor::WorkerInterceptor) = [] of Temporalio::Interceptor::WorkerInterceptor
      ) : WorkflowInstance
        obj = ConcreteWorkflowObject(T).new
        WorkflowInstance.new(activation, obj, data_converter, namespace, task_queue, interceptors)
      end
    end

    # Manages the set of live WorkflowInstance objects for a worker.
    class WorkflowRunner
      def initialize(
        @data_converter : DataConverter,
        @namespace : String,
        @task_queue : String,
        @interceptors : Array(Temporalio::Interceptor::WorkerInterceptor) = [] of Temporalio::Interceptor::WorkerInterceptor
      )
        @instances = {} of String => WorkflowInstance
        @definitions = {} of String => WorkflowDefinition
      end

      # Register a workflow definition.
      def register(defn : WorkflowDefinition) : Nil
        @definitions[defn.workflow_name] = defn
      end

      # Process a raw activation bytes from Core. Returns completion bytes to send back.
      def handle_activation(bytes : Bytes) : Bytes
        activation = Coresdk::WorkflowActivation::WorkflowActivation.from_protobuf(IO::Memory.new(bytes))
        run_id = activation.run_id || ""
        jobs = activation.jobs || [] of Coresdk::WorkflowActivation::WorkflowActivationJob
        jobs.each_with_index do |job, i|
          job_type = case
          when job.initialize_workflow then "InitializeWorkflow"
          when job.fire_timer then "FireTimer"
          when job.update_random_seed then "UpdateRandomSeed"
          when job.query_workflow then "QueryWorkflow"
          when job.cancel_workflow then "CancelWorkflow"
          when job.signal_workflow then "SignalWorkflow"
          when job.resolve_activity then "ResolveActivity"
          when job.resolve_child_workflow_execution then "ResolveChildWorkflowExecution"
          when job.do_update then "DoUpdate"
          when job.remove_from_cache then "RemoveFromCache"
          when job.notify_has_patch then "NotifyHasPatch"
          when job.resolve_child_workflow_execution_start then "ResolveChildWorkflowExecutionStart"
          when job.resolve_signal_external_workflow then "ResolveSignalExternalWorkflow"
          when job.resolve_request_cancel_external_workflow then "ResolveRequestCancelExternalWorkflow"
          else "Unknown"
          end
        end

        instance = @instances[run_id]?

        if instance.nil?
          jobs = activation.jobs || [] of Coresdk::WorkflowActivation::WorkflowActivationJob
          init_job = jobs.find { |j| j.initialize_workflow }
          unless init_job
            return empty_completion(run_id)
          end

          init = init_job.initialize_workflow.not_nil!
          wf_type = init.workflow_type || ""
          defn = @definitions[wf_type]?
          unless defn
            return fail_completion(run_id, "Unknown workflow type: #{wf_type}")
          end

          instance = defn.new_instance(activation, @data_converter, @namespace, @task_queue, @interceptors)
          @instances[run_id] = instance
        end

        completion = instance.apply_activation(activation)

        @instances.delete(run_id) if instance.complete?

        completion.to_protobuf.to_slice
      rescue ex
        fail_completion("", ex.message || ex.class.name)
      end

      def cached_size : Int32
        @instances.size
      end

      private def empty_completion(run_id : String) : Bytes
        Coresdk::WorkflowCompletion::WorkflowActivationCompletion.new(
          run_id: run_id,
          successful: Coresdk::WorkflowCompletion::Success.new(
            commands: [] of Coresdk::WorkflowCommands::WorkflowCommand
          )
        ).to_protobuf.to_slice
      end

      private def fail_completion(run_id : String, message : String) : Bytes
        failure = Temporal::Api::Failure::V1::Failure.new(
          message: message,
          application_failure_info: Temporal::Api::Failure::V1::ApplicationFailureInfo.new(
            type: "InternalError",
            non_retryable: true
          )
        )
        Coresdk::WorkflowCompletion::WorkflowActivationCompletion.new(
          run_id: run_id,
          failed: Coresdk::WorkflowCompletion::Failure.new(failure: failure)
        ).to_protobuf.to_slice
      end
    end
  end
end
