class ChildWorkflow
  include Temporalio::Workflow

  workflow_name "ChildWorkflow"

  def execute(name : String) : String
    "child:#{name}"
  end

end

class ParentWorkflow
  include Temporalio::Workflow

  workflow_name "ParentWorkflow"

  def execute(name : String) : String
    workflow.execute_child_workflow(ChildWorkflow, name, task_queue: workflow.task_queue)
  end

end

class FailingChildWorkflow
  include Temporalio::Workflow

  workflow_name "FailingChildWorkflow"

  def execute : String
    raise Temporalio::ApplicationError.new("child failed", type: "ChildError", non_retryable: true)
  end

end

class ParentWithFailingChildWorkflow
  include Temporalio::Workflow

  workflow_name "ParentWithFailingChildWorkflow"

  def execute : String
    begin
      workflow.execute_child_workflow(FailingChildWorkflow, task_queue: workflow.task_queue)
      "should not reach"
    rescue ex : Temporalio::Error
      "caught:#{ex.message}"
    end
  end

end
