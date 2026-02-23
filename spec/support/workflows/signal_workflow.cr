class SingleSignalWorkflow
  include Temporalio::Workflow

  workflow_name "SingleSignalWorkflow"

  @received : String = ""

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    ctx.wait_condition { !@received.empty? }
    @received
  end


  workflow_signal("my-signal", String) do |value|
    @received = value
  end
end

class MultiSignalWorkflow
  include Temporalio::Workflow

  workflow_name "MultiSignalWorkflow"

  @count : Int64 = 0_i64
  @name : String = ""

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    ctx.wait_condition { @count > 0 && !@name.empty? }
    "#{@name}:#{@count}"
  end


  workflow_signal("increment", Int64) do |n|
    @count += n
  end

  workflow_signal("set-name", String) do |name|
    @name = name
  end
end

class CancelAfterSignalWorkflow
  include Temporalio::Workflow

  workflow_name "CancelAfterSignalWorkflow"

  @signaled : Bool = false

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    ctx.wait_condition { @signaled }
    ctx.check_cancellation!
    "should not reach here"
  end


  workflow_signal("trigger") do
    @signaled = true
  end
end
