class ActivityRetryWorkflow
  include Temporalio::Workflow

  workflow_name "ActivityRetryWorkflow"

  def execute(attempt_to_succeed : Int64) : String
    retry_policy = Temporalio::Client::RetryPolicy.new(
      initial_interval: 100.milliseconds,
      maximum_interval: 1.second,
      maximum_attempts: 10
    )
    result = workflow.execute_activity(
      RetryableActivity,
      attempt_to_succeed,
      start_to_close_timeout: 10.seconds,
      retry_policy: retry_policy
    )
    "Succeeded after retries: #{result}, attempt #{attempt_to_succeed}"
  end
end
