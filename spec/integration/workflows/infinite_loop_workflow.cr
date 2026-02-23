class InfiniteLoopWorkflow
  include Temporalio::Workflow
  
  workflow_name "InfiniteLoopWorkflow"
  
  def execute : String
    ctx = Temporalio::Workflow::Context.current
    
    # Loop forever - workflow execution timeout should kill it
    loop do
      ctx.sleep(100.milliseconds)
    end
    
    "Should not reach here"
  end
end
