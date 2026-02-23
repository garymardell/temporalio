class ActivityWorkflow
  include Temporalio::Workflow

  workflow_name "ActivityWorkflow"

  def execute(name : String) : String
    workflow.execute_activity(HelloActivity, name, start_to_close_timeout: 10.seconds)
  end

end

class SequentialActivitiesWorkflow
  include Temporalio::Workflow

  workflow_name "SequentialActivitiesWorkflow"

  def execute(n : Int64) : Int64
    total = 0_i64
    n.times do |i|
      result = workflow.execute_activity(HelloActivity, "item#{i}", start_to_close_timeout: 10.seconds)
      total += result.not_nil!.size.to_i64
    end
    total
  end

end

class FailingActivityWorkflow
  include Temporalio::Workflow

  workflow_name "FailingActivityWorkflow"

  def execute : String
    workflow.execute_activity(FailingActivity, "expected failure", start_to_close_timeout: 10.seconds)
    "should not reach"
  end

end

class LocalActivityWorkflow
  include Temporalio::Workflow

  workflow_name "LocalActivityWorkflow"

  def execute(name : String) : String
    workflow.execute_local_activity(HelloActivity, name, start_to_close_timeout: 10.seconds)
  end

end
