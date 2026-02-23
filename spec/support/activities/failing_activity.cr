class FailingActivity
  include Temporalio::Activity

  activity_name "FailingActivity"

  def execute(message : String) : String
    raise Temporalio::ApplicationError.new(message, type: "TestError", non_retryable: true)
  end
end
