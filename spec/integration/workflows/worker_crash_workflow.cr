class WorkerCrashWorkflow
  include Temporalio::Workflow

  workflow_name "WorkerCrashWorkflow"

  def initialize
    @step = 0
    @restart_signal_received = false
  end

  workflow_signal "restart" do
    @restart_signal_received = true
  end

  def execute : Int64
    ctx = Temporalio::Workflow::Context.current
    
    @step = 1
    ctx.sleep(100.milliseconds)
    
    @step = 2
    ctx.sleep(100.milliseconds)
    
    # Wait for restart signal (simulates crash/recovery)
    @step = 3
    ctx.wait_condition { @restart_signal_received }
    
    @step = 4
    ctx.sleep(100.milliseconds)
    
    @step = 5
    @step.to_i64
  end
end
