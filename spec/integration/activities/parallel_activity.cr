class ParallelActivity
  include Temporalio::Activity

  activity_name "ParallelActivity"

  def execute(value : Int64) : Int64
    # Simulate some work
    sleep(50.milliseconds)
    value * 10
  end
end
