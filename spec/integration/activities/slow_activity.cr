class SlowActivity
  include Temporalio::Activity
  
  activity_name "SlowActivity"
  
  def execute(message : String) : String
    # Sleep for much longer than the timeout
    sleep 10.seconds
    "completed: #{message}"
  end
end
