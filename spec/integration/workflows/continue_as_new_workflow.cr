class ContinueAsNewWorkflow
  include Temporalio::Workflow

  workflow_name "ContinueAsNewWorkflow"

  def execute(counter : Int64, max : Int64) : Int64
    if counter >= max
      return counter
    end
    
    ctx = Temporalio::Workflow::Context.current
    # Continue as new with incremented counter
    ctx.continue_as_new(counter + 1, max)
  end
end
