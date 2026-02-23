class PatchedBehaviorWorkflow
  include Temporalio::Workflow

  workflow_name "PatchedBehaviorWorkflow"

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    if ctx.patched?("v2-behavior")
      "new-behavior"
    else
      "old-behavior"
    end
  end

end

class MultiPatchWorkflow
  include Temporalio::Workflow

  workflow_name "MultiPatchWorkflow"

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    parts = [] of String
    parts << "v2" if ctx.patched?("patch-v2")
    parts << "v3" if ctx.patched?("patch-v3")
    parts.empty? ? "v1" : parts.join(",")
  end

end
