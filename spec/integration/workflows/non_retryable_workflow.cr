class NonRetryableWorkflow
  include Temporalio::Workflow

  workflow_name "NonRetryableWorkflow"

  def execute : String
    # Execute activity that will fail with non-retryable error
    workflow.execute_activity(NonRetryableActivity, "test", start_to_close_timeout: 10.seconds)

    "Should not reach here"
  end
end
