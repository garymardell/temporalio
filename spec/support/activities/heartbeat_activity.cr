class HeartbeatActivity
  include Temporalio::Activity

  activity_name "HeartbeatActivity"

  def execute(count : Int64) : Int64
    count.times do |i|
      activity.heartbeat(i.to_i64)
      activity.check_cancellation!
    end
    count
  end
end
