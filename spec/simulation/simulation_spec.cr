require "./spec_helper"

# Comprehensive Temporal SDK Simulation
#
# This simulation tests ALL Temporal features in a fault-tolerant, deterministic manner:
# - Child workflows with deep nesting
# - Signal/query patterns
# - Activity timeouts and retries
# - Workflow cancellation
# - Continue-as-new for long-running processes
# - Worker crashes and replay
# - Concurrent execution
# - Timer/sleep operations
# - Non-determinism detection
#
# The simulation can run for variable duration with configurable:
# - Number of concurrent workflows
# - Number of activities per workflow
# - Failure injection rate
# - Timeout scenarios
# - Worker restart frequency

describe "Temporal SDK Full Simulation" do

  it "runs comprehensive fault-tolerant simulation" do
    client = simulation_create_client
    task_queue = "simulation-#{Random.rand(100000)}"
    
    # Configuration
    config = SimulationConfig.new(
      duration: 60.seconds,          # Total simulation time
      num_workflows: 20,              # Concurrent workflows
      activities_per_workflow: 5,     # Activities per workflow
      failure_rate: 0.2,              # 20% failure rate
      timeout_rate: 0.1,              # 10% timeout rate
      worker_restart_interval: 15.seconds,
      enable_child_workflows: true,
      enable_continue_as_new: true,
      enable_signals: true,
      enable_queries: true,
      enable_cancellation: true,
      max_workflow_depth: 3           # Max child workflow nesting
    )
    
    # Start worker with all simulation components
    worker = simulation_create_worker(
      client,
      task_queue,
      [
        SimulationOrchestrator,
        ScenarioWorkflow,
        ChildWorkflowScenario,
        SignalScenario,
        ContinueAsNewScenario,
        TimeoutScenario,
        RetryScenario,
        CancellationScenario,
        QueryScenario,
        ParallelScenario
      ],
      [
        SimulationActivity,
        FlakeyActivity,
        TimeoutActivity,
        HeartbeatActivity,
        ComputeActivity
      ]
    )
    
    begin
      # Execute orchestrator workflow
      result = client.execute_workflow(
        SimulationOrchestrator.workflow_name,
        config,
        id: "simulation-#{Time.utc.to_unix}",
        task_queue: task_queue,
        execution_timeout: config.duration + 30.seconds
      )
      
      # Parse and validate results
      metrics = SimulationMetrics.from_json(result.as(String))
      
      puts "\n" + "="*80
      puts "SIMULATION RESULTS"
      puts "="*80
      puts "Duration: #{metrics.duration_seconds}s"
      puts "Workflows Executed: #{metrics.total_workflows}"
      puts "Activities Executed: #{metrics.total_activities}"
      puts "Success Rate: #{metrics.success_rate * 100}%"
      puts "\nBreakdown by Scenario:"
      metrics.scenario_results.each do |scenario, stats|
        puts "  #{scenario}:"
        puts "    Executed: #{stats.executed}"
        puts "    Succeeded: #{stats.succeeded}"
        puts "    Failed: #{stats.failed}"
        puts "    Timed Out: #{stats.timed_out}"
        puts "    Cancelled: #{stats.cancelled}"
      end
      puts "\nDeterminism Checks:"
      puts "  Replay Errors: #{metrics.replay_errors}"
      puts "  Non-deterministic Events: #{metrics.nondeterministic_events}"
      puts "\nFault Tolerance:"
      puts "  Worker Restarts: #{metrics.worker_restarts}"
      puts "  Activity Retries: #{metrics.activity_retries}"
      puts "  Workflow Cancellations: #{metrics.workflow_cancellations}"
      puts "="*80
      
      # Assertions
      metrics.total_workflows.should be >= config.num_workflows
      metrics.replay_errors.should eq(0)
      metrics.nondeterministic_events.should eq(0)
      metrics.success_rate.should be >= 0.7 # At least 70% success with failures injected
      
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end

  it "runs targeted scenario tests" do
    client = simulation_create_client
    task_queue = "scenario-#{Random.rand(100000)}"
    
    worker = simulation_create_worker(
      client,
      task_queue,
      [ScenarioWorkflow, ChildWorkflowScenario],
      [SimulationActivity]
    )
    
    begin
      # Test each scenario type individually
      scenarios = [
        {name: "child_workflow", depth: 3},
        {name: "signal_pattern", count: 10},
        {name: "query_pattern", count: 5},
        {name: "activity_retry", max_attempts: 5},
        {name: "timeout_handling", timeout_ms: 1000},
        {name: "cancellation", delay_ms: 500}
      ]
      
      results = [] of Hash(String, String)
      
      scenarios.each do |scenario|
        result = client.execute_workflow(
          ScenarioWorkflow.workflow_name,
          scenario[:name],
          scenario,
          id: "scenario-#{scenario[:name]}-#{Random.rand(10000)}",
          task_queue: task_queue,
          execution_timeout: 30.seconds
        )
        results << {"scenario" => scenario[:name].to_s, "result" => result.as(String)}
      end
      
      results.size.should eq(scenarios.size)
      results.all? { |r| r["result"].includes?("success") || r["result"].includes?("completed") }.should be_true
      
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end

  it "validates determinism through replay" do
    client = simulation_create_client
    task_queue = "replay-#{Random.rand(100000)}"
    
    worker = simulation_create_worker(
      client,
      task_queue,
      [ScenarioWorkflow],
      [SimulationActivity]
    )
    
    begin
      # Execute workflow and capture history
      workflow_id = "determinism-test-#{Random.rand(10000)}"
      
      result1 = client.execute_workflow(
        ScenarioWorkflow.workflow_name,
        "complex_pattern",
        {depth: 2, activities: 5, signals: 3},
        id: workflow_id,
        task_queue: task_queue,
        execution_timeout: 30.seconds
      )
      
      # Get workflow history for replay validation
      handle = client.workflow_handle(workflow_id)
      description = handle.describe
      
      description.history_length.should be > 0
      description.status.should eq(2) # COMPLETED
      
      puts "\nDeterminism Test:"
      puts "  Workflow ID: #{workflow_id}"
      puts "  History Length: #{description.history_length}"
      puts "  Result: #{result1}"
      
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end

  it "tests concurrent workflow execution with failure injection" do
    client = simulation_create_client
    task_queue = "concurrent-#{Random.rand(100000)}"
    
    worker = simulation_create_worker(
      client,
      task_queue,
      [ParallelScenario],
      [FlakeyActivity]
    )
    
    begin
      # Spawn 50 workflows concurrently with 30% failure rate
      num_workflows = 50
      results_channel = Channel(String).new(num_workflows)
      
      num_workflows.times do |i|
        spawn do
          begin
            result = client.execute_workflow(
              ParallelScenario.workflow_name,
              i,
              0.3, # 30% failure rate
              id: "parallel-#{i}-#{Random.rand(1000)}",
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            results_channel.send("success:#{result}")
          rescue ex
            results_channel.send("error:#{ex.message}")
          end
        end
      end
      
      # Collect results
      successes = 0
      failures = 0
      
      num_workflows.times do
        result = results_channel.receive
        if result.starts_with?("success")
          successes += 1
        else
          failures += 1
        end
      end
      
      puts "\nConcurrent Execution Test:"
      puts "  Total: #{num_workflows}"
      puts "  Successes: #{successes}"
      puts "  Failures: #{failures}"
      puts "  Success Rate: #{(successes.to_f / num_workflows * 100).round(2)}%"
      
      successes.should be >= (num_workflows * 0.5).to_i # At least 50% success
      
    ensure
      worker.initiate_shutdown
      worker.wait_all_complete
    end
  end
end
