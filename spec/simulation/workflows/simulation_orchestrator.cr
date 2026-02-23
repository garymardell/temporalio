require "../simulation_config"

# Main orchestrator workflow that coordinates the entire simulation
class SimulationOrchestrator
  include Temporalio::Workflow

  workflow_name "SimulationOrchestrator"

  @metrics : SimulationMetrics
  @start_time : Time

  def initialize
    @metrics = SimulationMetrics.new
    @start_time = Time.utc
  end

  def execute(config : SimulationConfig) : String
    @start_time = workflow.now

    # Calculate end time
    end_time = @start_time + config.duration

    # Track child workflow handles for coordination
    child_handles = [] of Hash(String, String)

    # Phase 1: Spawn initial workflow batch
    config.num_workflows.times do |i|
      scenario_type = select_scenario_type(i, config)

      begin
        handle = spawn_scenario_workflow(scenario_type, i, config)
        child_handles << {
          "id" => i.to_s,
          "type" => scenario_type,
          "status" => "running"
        }
      rescue ex
        @metrics.replay_errors += 1
      end
    end

    # Phase 2: Monitor and manage workflows
    loop do
      current_time = workflow.now
      break if current_time >= end_time

      # Sleep for monitoring interval
      workflow.sleep(5.seconds)

      # Optionally spawn more workflows during simulation
      if Random.rand < 0.2 # 20% chance to spawn additional workflow
        scenario_type = select_scenario_type(child_handles.size, config)
        handle = spawn_scenario_workflow(scenario_type, child_handles.size, config)
        child_handles << {
          "id" => child_handles.size.to_s,
          "type" => scenario_type,
          "status" => "running"
        }
      end

      # Random cancellation if enabled
      if config.enable_cancellation && Random.rand < 0.05
        # Cancel a random running workflow
        running = child_handles.select { |h| h["status"] == "running" }
        if running.size > 0
          target = running.sample
          target["status"] = "cancelled"
          @metrics.workflow_cancellations += 1
        end
      end
    end

    # Phase 3: Collect final metrics
    @metrics.duration_seconds = (workflow.now - @start_time).total_seconds
    @metrics.total_workflows = child_handles.size

    # Calculate success rate
    total = @metrics.scenario_results.values.sum(&.executed)
    successes = @metrics.scenario_results.values.sum(&.succeeded)
    @metrics.success_rate = total > 0 ? successes.to_f / total : 0.0

    @metrics.to_json
  end

  private def select_scenario_type(index : Int32, config : SimulationConfig) : String
    scenarios = [] of String
    scenarios << "child_workflow" if config.enable_child_workflows
    scenarios << "signal_pattern" if config.enable_signals
    scenarios << "query_pattern" if config.enable_queries
    scenarios << "activity_retry"
    scenarios << "timeout_handling"
    scenarios << "parallel_execution"
    scenarios << "continue_as_new" if config.enable_continue_as_new

    scenarios[index % scenarios.size]
  end

  private def spawn_scenario_workflow(
    scenario_type : String,
    index : Int32,
    config : SimulationConfig
  )
    # Initialize stats for scenario if not exists
    unless @metrics.scenario_results.has_key?(scenario_type)
      @metrics.scenario_results[scenario_type] = ScenarioStats.new
    end

    start_time = workflow.now

    begin
      # Execute child workflow based on scenario type
      result = workflow.execute_child_workflow(
        ScenarioWorkflow,
        scenario_type,
        {
          "index" => JSON::Any.new(index.to_i64),
          "failure_rate" => JSON::Any.new(config.failure_rate),
          "timeout_rate" => JSON::Any.new(config.timeout_rate),
          "activities" => JSON::Any.new(config.activities_per_workflow.to_i64),
          "max_depth" => JSON::Any.new(config.max_workflow_depth.to_i64)
        } of String => JSON::Any,
        workflow_id: "scenario-#{scenario_type}-#{index}",
        execution_timeout: 60.seconds
      )

      duration_ms = (workflow.now - start_time).total_milliseconds
      @metrics.scenario_results[scenario_type].record_success(duration_ms)
      @metrics.total_activities += config.activities_per_workflow

      result
    rescue ex : Temporalio::CancelledError
      @metrics.scenario_results[scenario_type].record_cancellation
      nil
    rescue ex : Temporalio::TimeoutError
      @metrics.scenario_results[scenario_type].record_timeout
      nil
    rescue ex
      @metrics.scenario_results[scenario_type].record_failure
      nil
    end
  end

  workflow_query "get_metrics" do
    @metrics.to_json
  end

  workflow_signal "inject_failure" do |scenario|
    if @metrics.scenario_results.has_key?(scenario.to_s)
      @metrics.scenario_results[scenario.to_s].record_failure
    end
  end
end
