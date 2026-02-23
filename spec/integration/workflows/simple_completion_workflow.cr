class SimpleCompletionWorkflow
  include Temporalio::Workflow

  workflow_name "SimpleCompletionWorkflow"

  def execute(name : String) : String
    "Hello, #{name}!"
  end
end
