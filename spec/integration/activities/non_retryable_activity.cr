class NonRetryableActivity
  include Temporalio::Activity
  
  activity_name "NonRetryableActivity"
  
  def execute(message : String) : String
    # Raise a non-retryable error
    raise Temporalio::ApplicationError.new(
      "This is a non-retryable error",
      type: "NonRetryableError",
      non_retryable: true
    )
  end
end
