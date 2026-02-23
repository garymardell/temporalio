require "./worker_options"
require "../ext/wrappers"

module Temporalio
  module Bridge
    # Bridge::Worker wraps the Rust extension Worker
    class Worker
      def initialize(client : Client, runtime : Runtime, options : WorkerOptions)
        # The Ext::Worker needs target, namespace, and task_queue
        # We can get these from the client and options
        @worker = Ext::Worker.new(
          target: client.target_url,
          namespace: client.namespace,
          task_queue: options.task_queue,
          max_cached_workflows: options.max_cached_workflows.to_i32
        )
      end

      def poll_workflow_activation : Bytes?
        @worker.poll_workflow_activation
      end

      def poll_activity_task : Bytes?
        @worker.poll_activity_task
      end

      def complete_workflow_activation(bytes : Bytes) : Nil
        @worker.complete_workflow_activation(bytes)
      end

      def complete_activity_task(bytes : Bytes) : Nil
        @worker.complete_activity_task(bytes)
      end

      def record_activity_heartbeat(bytes : Bytes) : String?
        # TODO: Implement heartbeat in Rust extension
        nil
      end

      def initiate_shutdown : Nil
        @worker.initiate_shutdown
      end

      def finalize_shutdown : Nil
        @worker.finalize_shutdown
      end
    end
  end
end
