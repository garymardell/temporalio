class ParallelActivitiesWorkflow
  include Temporalio::Workflow

  workflow_name "ParallelActivitiesWorkflow"

  def execute(count : Int64) : Array(Int64)
    # Execute multiple activities sequentially
    results = [] of Int64

    count.times do |i|
      result = workflow.execute_activity(ParallelActivity, i.to_i64, start_to_close_timeout: 10.seconds)
      results << result.not_nil!
    end

    results
  end
end
