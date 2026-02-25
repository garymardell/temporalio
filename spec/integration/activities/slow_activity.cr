class SlowActivity
  include Temporalio::Activity
  
  activity_name "SlowActivity"
  
  def execute(message : String) : String
    # Sleep in small increments so cancellation is detected quickly when timed out
    20.times do
      activity.check_cancellation!
      sleep 100.milliseconds
    end
    "completed: #{message}"
  end
end
