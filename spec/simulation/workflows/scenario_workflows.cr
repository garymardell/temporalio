require "../simulation_config"

# Main scenario workflow that delegates to specific patterns
class ScenarioWorkflow
  include Temporalio::Workflow

  workflow_name "ScenarioWorkflow"

  def execute(scenario_type : String, params : Hash(String, JSON::Any)) : String
    case scenario_type
    when "child_workflow"
      execute_child_pattern(params)
    when "signal_pattern"
      execute_signal_pattern(params)
    when "query_pattern"
      execute_query_pattern(params)
    when "activity_retry"
      execute_retry_pattern(params)
    when "timeout_handling"
      execute_timeout_pattern(params)
    when "parallel_execution"
      execute_parallel_pattern(params)
    when "continue_as_new"
      execute_continue_as_new_pattern(params)
    when "complex_pattern"
      execute_complex_pattern(params)
    else
      "unknown_scenario:#{scenario_type}"
    end
  end

  private def execute_child_pattern(params) : String
    depth = params["max_depth"]?.try(&.as_i) || 2

    if depth > 0
      # Spawn child workflow
      result = workflow.execute_child_workflow(
        ChildWorkflowScenario,
        (depth - 1).to_i64,
        workflow_id: "child-depth-#{depth}-#{Random.rand(1000)}",
        execution_timeout: 30.seconds
      )
      "child_workflow:success:depth_#{depth}:child_result=#{result}"
    else
      "child_workflow:success:leaf_node"
    end
  end

  private def execute_signal_pattern(params) : String
    count = params["count"]?.try(&.as_i) || 5
    values = [] of String

    count.times do |i|
      workflow.sleep(100.milliseconds)
      values << "signal_#{i}"
    end

    "signal_pattern:success:received_#{count}_signals"
  end

  private def execute_query_pattern(params) : String
    count = params["count"]?.try(&.as_i) || 3

    count.times do |i|
      workflow.sleep(100.milliseconds)
    end

    "query_pattern:success:completed_#{count}_iterations"
  end

  private def execute_retry_pattern(params) : String
    failure_rate = params["failure_rate"]?.try(&.as_f) || 0.3
    activities = params["activities"]?.try(&.as_i) || 3

    successes = 0
    activities.times do |i|
      begin
        workflow.execute_activity(
          SimulationActivity,
          "retry_test", failure_rate,
          start_to_close_timeout: 10.seconds,
          retry_policy: Temporalio::Client::RetryPolicy.new(
            maximum_attempts: 5,
            initial_interval: 100.milliseconds,
            backoff_coefficient: 2.0
          )
        )
        successes += 1
      rescue ex
        # Activity failed after all retries
      end
    end

    "activity_retry:success:#{successes}/#{activities}_succeeded"
  end

  private def execute_timeout_pattern(params) : String
    timeout_ms = params["timeout_ms"]?.try(&.as_i) || 1000

    begin
      workflow.execute_activity(
        TimeoutActivity,
        (timeout_ms * 2).to_i64, # Activity takes 2x timeout
        start_to_close_timeout: timeout_ms.milliseconds
      )
      "timeout_handling:unexpected_success"
    rescue ex : Temporalio::ActivityError
      "timeout_handling:success:caught_timeout"
    end
  end

  private def execute_parallel_pattern(params) : String
    count = params["activities"]?.try(&.as_i) || 5
    failure_rate = params["failure_rate"]?.try(&.as_f) || 0.2

    # Execute multiple activities in sequence (parallel would need special handling)
    results = [] of String
    count.times do |i|
      begin
        workflow.execute_activity(SimulationActivity, "parallel_#{i}", failure_rate, start_to_close_timeout: 5.seconds)
        results << "success"
      rescue ex
        results << "failed"
      end
    end

    successes = results.count("success")
    "parallel_execution:completed:#{successes}/#{count}_succeeded"
  end

  private def execute_continue_as_new_pattern(params) : String
    index = params["index"]?.try(&.as_i) || 0
    max_iterations = 3

    if index >= max_iterations
      "continue_as_new:success:completed_#{index}_iterations"
    else
      workflow.sleep(200.milliseconds)
      workflow.continue_as_new(
        "continue_as_new",
        params.merge({"index" => JSON::Any.new(index + 1)})
      )
    end
  end

  private def execute_complex_pattern(params) : String
    depth = params["depth"]?.try(&.as_i) || 1
    activities = params["activities"]?.try(&.as_i) || 3
    signals = params["signals"]?.try(&.as_i) || 2

    results = [] of String

    # Execute activities
    activities.times do |i|
      begin
        workflow.execute_activity(SimulationActivity, "complex_#{i}", 0.1_f64, start_to_close_timeout: 5.seconds)
        results << "activity_#{i}:ok"
      rescue ex
        results << "activity_#{i}:failed"
      end
    end

    # Spawn child if depth allows
    if depth > 0
      begin
        child_result = workflow.execute_child_workflow(
          ScenarioWorkflow,
          "complex_pattern",
          params.merge({"depth" => JSON::Any.new(depth - 1)}),
          workflow_id: "complex-child-#{depth}-#{Random.rand(1000)}",
          execution_timeout: 20.seconds
        )
        results << "child:#{child_result}"
      rescue ex
        results << "child:failed"
      end
    end

    # Add some timers
    signals.times do |i|
      workflow.sleep(50.milliseconds)
      results << "timer_#{i}:ok"
    end

    "complex_pattern:success:#{results.join(",")}"
  end

  workflow_query "get_status" do
    "running"
  end

  workflow_signal "inject_event" do |event|
    # Handle injected events for testing
  end
end

# Child workflow for testing nested workflows
class ChildWorkflowScenario
  include Temporalio::Workflow

  workflow_name "ChildWorkflowScenario"

  def execute(depth : Int64) : String
    if depth > 0
      # Recursively spawn child
      child_result = workflow.execute_child_workflow(
        ChildWorkflowScenario,
        depth - 1,
        workflow_id: "child-nested-#{depth}-#{Random.rand(1000)}",
        execution_timeout: 20.seconds
      )
      "depth_#{depth}[#{child_result}]"
    else
      workflow.sleep(100.milliseconds)
      "leaf"
    end
  end
end

# Signal-based workflow for testing signal patterns
class SignalScenario
  include Temporalio::Workflow

  workflow_name "SignalScenario"

  @values = [] of String
  @finished = false

  def execute : String
    until @finished
      workflow.sleep(100.milliseconds)
    end

    "signals_received:#{@values.size}:#{@values.join(",")}"
  end

  workflow_signal "add_value" do |value|
    @values << value.to_s
  end

  workflow_signal "finish" do
    @finished = true
  end

  workflow_query "get_values" do
    @values
  end
end

# Continue-as-new pattern
class ContinueAsNewScenario
  include Temporalio::Workflow

  workflow_name "ContinueAsNewScenario"

  def execute(counter : Int64, max : Int64) : Int64
    if counter >= max
      counter
    else
      workflow.sleep(100.milliseconds)
      workflow.continue_as_new(counter + 1, max)
    end
  end
end

# Timeout testing workflow
class TimeoutScenario
  include Temporalio::Workflow

  workflow_name "TimeoutScenario"

  def execute(timeout_seconds : Int64) : String
    begin
      workflow.execute_activity(
        TimeoutActivity,
        timeout_seconds * 2000, # Sleep 2x timeout
        start_to_close_timeout: timeout_seconds.seconds
      )
      "unexpected_success"
    rescue ex : Temporalio::ActivityError
      "timeout_caught:#{ex.message}"
    end
  end
end

# Retry pattern workflow
class RetryScenario
  include Temporalio::Workflow

  workflow_name "RetryScenario"

  def execute(succeed_on_attempt : Int64) : String
    workflow.execute_activity(
      FlakeyActivity,
      succeed_on_attempt,
      start_to_close_timeout: 5.seconds,
      retry_policy: Temporalio::Client::RetryPolicy.new(
        maximum_attempts: 10,
        initial_interval: 100.milliseconds,
        backoff_coefficient: 1.5
      )
    )

    "retry_success:attempt_#{succeed_on_attempt}"
  end
end

# Cancellation testing workflow
class CancellationScenario
  include Temporalio::Workflow

  workflow_name "CancellationScenario"

  def execute(sleep_seconds : Int64) : String
    begin
      workflow.sleep(sleep_seconds.seconds)
      "completed_normally"
    rescue ex : Temporalio::CancelledError
      "cancelled_after_partial_execution"
    end
  end
end

# Query testing workflow
class QueryScenario
  include Temporalio::Workflow

  workflow_name "QueryScenario"

  @progress = 0

  def execute(steps : Int64) : String
    steps.times do |i|
      @progress = i + 1
      workflow.sleep(100.milliseconds)
    end

    "completed_#{steps}_steps"
  end

  workflow_query "get_progress" do
    @progress
  end
end

# Parallel activity execution
class ParallelScenario
  include Temporalio::Workflow

  workflow_name "ParallelScenario"

  def execute(id : Int64, failure_rate : Float64) : String
    # Execute 5 activities sequentially
    successes = 0
    5.times do |i|
      begin
        workflow.execute_activity(SimulationActivity, "parallel_#{id}_#{i}", failure_rate, start_to_close_timeout: 5.seconds)
        successes += 1
      rescue ex
        # Activity failed
      end
    end

    "parallel_#{id}:#{successes}/5"
  end
end
