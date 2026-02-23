class SimpleWorkflow
  include Temporalio::Workflow

  workflow_name "SimpleWorkflow"

  def execute(name : String) : String
    "Hello, #{name}!"
  end

end

class PatchedWorkflow
  include Temporalio::Workflow

  workflow_name "PatchedWorkflow"

  def execute : Bool
    ctx = Temporalio::Workflow::Context.current
    ctx.patched?("my-patch")
  end

end
