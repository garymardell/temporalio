class ChildWorkflowParent
  include Temporalio::Workflow

  workflow_name "ChildWorkflowParent"

  def execute(child_name : String) : String
    result = workflow.execute_child_workflow(ChildWorkflowChild, child_name, workflow_id: "child-#{workflow.workflow_id}")
    "Parent received: #{result}"
  end
end

class ChildWorkflowChild
  include Temporalio::Workflow

  workflow_name "ChildWorkflowChild"

  def execute(name : String) : String
    "Child says hello to #{name}"
  end
end
