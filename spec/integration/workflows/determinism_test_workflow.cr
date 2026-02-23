class DeterminismTestWorkflow
  include Temporalio::Workflow

  workflow_name "DeterminismTestWorkflow"

  def initialize
    @execution_count = 0
  end

  def execute(iterations : Int64) : String
    results = [] of String

    iterations.times do |i|
      @execution_count += 1

      # Timer - must be deterministic
      workflow.sleep(10.milliseconds)
      results << "timer-#{i}"

      # Activity - must be deterministic
      result = workflow.execute_activity(DeterministicActivity, i.to_i64, start_to_close_timeout: 5.seconds)
      results << "activity-#{result}"
    end

    "Executions: #{@execution_count}, Results: #{results.join(",")}"
  end
end
