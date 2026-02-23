class ContinueAsNewWorkflow
  include Temporalio::Workflow

  workflow_name "ContinueAsNewWorkflow"

  def execute(count : Int64) : Int64
    return count if count <= 0
    ctx = Temporalio::Workflow::Context.current
    ctx.continue_as_new(count - 1)
    count # unreachable
  end

end

class ContinueAsNewAccumulatorWorkflow
  include Temporalio::Workflow

  workflow_name "ContinueAsNewAccumulatorWorkflow"

  def execute(remaining : Int64, accumulated : Int64) : Int64
    return accumulated if remaining <= 0
    ctx = Temporalio::Workflow::Context.current
    ctx.continue_as_new(remaining - 1, accumulated + remaining)
    accumulated # unreachable
  end

end
