class TimerWorkflow
  include Temporalio::Workflow

  workflow_name "TimerWorkflow"

  def execute(sleep_duration_ms : Int64) : String
    ctx = Temporalio::Workflow::Context.current
    start_time = ctx.now
    ctx.sleep(sleep_duration_ms.milliseconds)
    end_time = ctx.now
    elapsed = (end_time - start_time).total_milliseconds.to_i64
    "Slept for approximately #{elapsed}ms"
  end
end
