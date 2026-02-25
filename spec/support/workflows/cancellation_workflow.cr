class UnitCancellationWorkflow
  include Temporalio::Workflow

  workflow_name "CancellationWorkflow"

  def execute : String
    workflow.sleep(1000.seconds)
    workflow.check_cancellation!
    "completed"
  end

end

class CancellationWithCleanupWorkflow
  include Temporalio::Workflow

  workflow_name "CancellationWithCleanupWorkflow"

  def execute : String
    workflow.wait_condition { workflow.cancelled? }
    "cancelled-and-cleaned-up"
  end

end

class CancellationDuringActivityWorkflow
  include Temporalio::Workflow

  workflow_name "CancellationDuringActivityWorkflow"

  def execute : String
    begin
      workflow.execute_activity(HeartbeatActivity, 100_i64, start_to_close_timeout: 60.seconds)
    rescue Temporalio::CancelledError
      return "activity-cancelled"
    end
    "completed"
  end

end
