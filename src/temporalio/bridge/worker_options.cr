module Temporalio
  module Bridge
    # Worker options for configuring a Temporal worker.
    # This is a compatibility wrapper - the actual implementation uses Ext::Worker.
    class WorkerOptions
      getter task_queue : String
      getter max_cached_workflows : Int32
      getter max_concurrent_workflow_tasks : Int32
      getter max_concurrent_activities : Int32
      getter max_concurrent_local_activities : Int32

      def initialize(
        @task_queue : String,
        @max_cached_workflows : Int32 = 1000,
        @max_concurrent_workflow_tasks : Int32 = 100,
        @max_concurrent_activities : Int32 = 100,
        @max_concurrent_local_activities : Int32 = 100
      )
      end
    end
  end
end
