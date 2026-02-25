class UnitTimerWorkflow
  include Temporalio::Workflow

  workflow_name "TimerWorkflow"

  def execute(seconds : Int64) : String
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(seconds.seconds)
    "slept #{seconds}s"
  end

end

class MultiTimerWorkflow
  include Temporalio::Workflow

  workflow_name "MultiTimerWorkflow"

  def execute(count : Int64) : Int64
    ctx = Temporalio::Workflow::Context.current
    count.times { ctx.sleep(1.second) }
    count
  end

end
