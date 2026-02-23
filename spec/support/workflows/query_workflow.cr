class SingleQueryWorkflow
  include Temporalio::Workflow

  workflow_name "SingleQueryWorkflow"

  @state : String = "initial"

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(1.second)
    @state
  end


  workflow_query("get-state") do
    @state
  end
end

class MultiQueryWorkflow
  include Temporalio::Workflow

  workflow_name "MultiQueryWorkflow"

  @count : Int64 = 42_i64
  @label : String = "hello"

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(1.second)
    "done"
  end


  workflow_query("get-count") do
    @count
  end

  workflow_query("get-label") do
    @label
  end
end

class QueryWithArgWorkflow
  include Temporalio::Workflow

  workflow_name "QueryWithArgWorkflow"

  @value : Int64 = 0_i64

  def execute : Int64
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(1.second)
    @value
  end


  workflow_query("multiply", Int64) do |factor|
    @value * factor
  end
end
