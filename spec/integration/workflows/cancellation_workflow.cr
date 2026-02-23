class CancellationWorkflow
  include Temporalio::Workflow

  workflow_name "CancellationWorkflow"

  def execute : String
    ctx = Temporalio::Workflow::Context.current
    begin
      # Sleep for a long time - should be cancelled before completion
      ctx.sleep(1.hour)
      "Completed normally"
    rescue ex : Temporalio::CancelledError
      "Cancelled as expected"
    end
  end
end
