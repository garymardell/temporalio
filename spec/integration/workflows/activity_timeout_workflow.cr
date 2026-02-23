class ActivityTimeoutWorkflow
  include Temporalio::Workflow

  workflow_name "ActivityTimeoutWorkflow"

  def execute : String
    # Execute activity with a very short timeout - will timeout
    workflow.execute_activity(SlowActivity, "test", start_to_close_timeout: 1.second)

    "Should not reach here"
  end
end
