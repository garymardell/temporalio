class SignalAccumulatorWorkflow
  include Temporalio::Workflow

  workflow_name "SignalAccumulatorWorkflow"

  def initialize
    @values = [] of String
    @done = false
  end

  workflow_signal "add_value", String do |value|
    @values << value
  end

  workflow_signal "finish" do
    @done = true
  end

  workflow_query "get_values", Array(String) do
    @values
  end

  def execute : Array(String)
    ctx = Temporalio::Workflow::Context.current
    ctx.wait_condition { @done }
    @values
  end
end
