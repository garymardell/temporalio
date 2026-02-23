require "../../../src/temporalio/workflow"

class SimpleUpdateWorkflow
  include Temporalio::Workflow

  @count : Int64 = 0_i64

  def self.workflow_name : String
    "SimpleUpdateWorkflow"
  end

  def execute : Int64
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(10.seconds)  # Keep running
    @count
  end

  workflow_update("increment", Int64) do |amount|
    @count += amount
    @count
  end
end

class ValidatedUpdateWorkflow
  include Temporalio::Workflow

  @balance : Int64 = 100_i64

  def self.workflow_name : String
    "ValidatedUpdateWorkflow"
  end

  def execute : Int64
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(10.seconds)
    @balance
  end

  workflow_update("withdraw", Int64) do |amount|
    @balance -= amount
    @balance
  end

  workflow_update_validator("withdraw", Int64) do |amount|
    raise "Insufficient funds" if amount > @balance
  end
end

class AsyncUpdateWorkflow
  include Temporalio::Workflow

  @messages : Array(String) = [] of String

  def self.workflow_name : String
    "AsyncUpdateWorkflow"
  end

  def execute : Array(String)
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(10.seconds)
    @messages
  end

  workflow_update("add_message", String) do |message|
    # Test that update handlers can use workflow operations
    ctx = Temporalio::Workflow::Context.current
    ctx.sleep(100.milliseconds)  # Simulate async work
    @messages << message
    @messages.size
  end
end
