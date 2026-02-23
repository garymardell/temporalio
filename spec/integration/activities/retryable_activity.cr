class RetryableActivity
  include Temporalio::Activity

  activity_name "RetryableActivity"

  @@attempt_counter = Hash(String, Int32).new(0)

  def execute(succeed_on_attempt : Int64) : String
    activity_id = activity.info.activity_id
    
    @@attempt_counter[activity_id] ||= 0
    @@attempt_counter[activity_id] += 1
    current_attempt = @@attempt_counter[activity_id]
    
    if current_attempt < succeed_on_attempt
      raise Temporalio::ApplicationError.new(
        "Attempt #{current_attempt}, will succeed on #{succeed_on_attempt}",
        type: "RetryableError",
        non_retryable: false
      )
    end
    
    @@attempt_counter.delete(activity_id)
    "Succeeded on attempt #{current_attempt}"
  end
end
