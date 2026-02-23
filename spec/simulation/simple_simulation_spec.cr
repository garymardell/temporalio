require "../spec_helper"
require "../../src/temporalio"

# Require all integration test workflows
require "../integration/workflows/simple_completion_workflow"
require "../integration/workflows/timer_workflow"
require "../integration/workflows/signal_accumulator_workflow"
require "../integration/workflows/activity_retry_workflow"
require "../integration/workflows/parallel_activities_workflow"
require "../integration/workflows/cancellation_workflow"
require "../integration/workflows/child_workflow_parent"
require "../integration/workflows/continue_as_new_workflow"
require "../integration/workflows/activity_timeout_workflow"
require "../integration/workflows/infinite_loop_workflow"
require "../integration/workflows/non_retryable_workflow"

# Require all integration test activities
require "../integration/activities/retryable_activity"
require "../integration/activities/parallel_activity"
require "../integration/activities/slow_activity"
require "../integration/activities/non_retryable_activity"

# Simplified 5-minute simulation using existing integration test workflows
# This version actually compiles and runs, providing comprehensive testing

module SimulationHelpers
  def self.create_client
    Temporalio::Client.connect(
      target_host: "http://localhost:7234",
      namespace: "default"
    )
  end
end

# Macro to create worker with correct types (same as integration tests)
macro create_worker(client, task_queue, workflows, activities)
  %workflow_defs = Array(Temporalio::Internal::WorkflowDefinition).new
  {% for wf in workflows %}
    %workflow_defs << Temporalio::Internal::ConcreteWorkflowDefinition({{wf}}).new
  {% end %}
  
  %activity_defs = Array(Temporalio::Internal::ActivityDefinition).new
  {% for act in activities %}
    %activity_defs << Temporalio::Internal::ConcreteActivityDefinition({{act}}).new
  {% end %}
  
  %worker = Temporalio::Worker.new(
    client: {{client}},
    task_queue: {{task_queue}},
    workflows: %workflow_defs,
    activities: %activity_defs
  )
  
  spawn { %worker.run }
  sleep 100.milliseconds # Give worker time to start
  %worker
end

describe "10-Minute Temporal SDK Simulation" do
  it "runs 10-minute comprehensive simulation" do
    puts "\n" + "="*80
    puts "TEMPORAL SDK 10-MINUTE SIMULATION"
    puts "="*80
    puts "Start Time: #{Time.utc}"
    puts "Duration: 10 minutes"
    puts "Features: All core Temporal patterns with fault injection"
    puts "="*80 + "\n"

    client = SimulationHelpers.create_client
    start_time = Time.utc
    end_time = start_time + 10.minutes
    
    # Metrics
    total_workflows = 0
    total_activities = 0
    successes = 0
    failures = 0
    timeouts = 0
    cancellations = 0
    
    scenario_stats = Hash(String, Hash(String, Int32)).new
    
    # Define scenario types with their configs
    scenarios = [
      {
        name: "simple_completion",
        workflow: SimpleCompletionWorkflow,
        activities: [] of Temporalio::Activity.class,
        weight: 20
      },
      {
        name: "timer_workflow",
        workflow: TimerWorkflow,
        activities: [] of Temporalio::Activity.class,
        weight: 15
      },
      {
        name: "signal_workflow",
        workflow: SignalAccumulatorWorkflow,
        activities: [] of Temporalio::Activity.class,
        weight: 10
      },
      {
        name: "activity_workflow",
        workflow: ActivityRetryWorkflow,
        activities: [RetryableActivity],
        weight: 15
      },
      {
        name: "multiple_activities",
        workflow: ParallelActivitiesWorkflow,
        activities: [ParallelActivity],
        weight: 10
      },
      {
        name: "cancellation",
        workflow: CancellationWorkflow,
        activities: [] of Temporalio::Activity.class,
        weight: 5
      },
      {
        name: "child_workflow",
        workflow: ChildWorkflowParent,
        activities: [] of Temporalio::Activity.class,
        weight: 10
      },
      {
        name: "continue_as_new",
        workflow: ContinueAsNewWorkflow,
        activities: [] of Temporalio::Activity.class,
        weight: 5
      },
      {
        name: "activity_timeout",
        workflow: ActivityTimeoutWorkflow,
        activities: [SlowActivity],
        weight: 5
      },
      {
        name: "workflow_timeout",
        workflow: InfiniteLoopWorkflow,
        activities: [] of Temporalio::Activity.class,
        weight: 3
      },
      {
        name: "non_retryable",
        workflow: NonRetryableWorkflow,
        activities: [NonRetryableActivity],
        weight: 2
      }
    ]
    
    # Initialize stats
    scenarios.each do |s|
      scenario_stats[s[:name].as(String)] = {
        "attempted" => 0,
        "succeeded" => 0,
        "failed" => 0,
        "timeout" => 0,
        "cancelled" => 0
      }
    end
    
    # Create a shared task queue
    task_queue = "simulation-#{Random.rand(100000)}"
    
    # Start worker with all workflows and activities
    worker = create_worker(
      client,
      task_queue,
      [
        SimpleCompletionWorkflow,
        TimerWorkflow,
        SignalAccumulatorWorkflow,
        ActivityRetryWorkflow,
        ParallelActivitiesWorkflow,
        CancellationWorkflow,
        ChildWorkflowParent,
        ChildWorkflowChild,
        ContinueAsNewWorkflow,
        ActivityTimeoutWorkflow,
        InfiniteLoopWorkflow,
        NonRetryableWorkflow
      ],
      [
        RetryableActivity,
        ParallelActivity,
        SlowActivity,
        NonRetryableActivity
      ]
    )
    
    # Progress tracking
    last_report = Time.utc
    iteration = 0
    
    begin
      puts "Starting simulation loop..."
      puts "Worker started with 12 workflows and 4 activities"
      puts ""
      
      # Main simulation loop
      while Time.utc < end_time
        iteration += 1
        
        # Select random scenario based on weights
        total_weight = scenarios.sum { |s| s[:weight].as(Int32) }
        random_weight = Random.rand(total_weight)
        cumulative = 0
        selected_scenario = scenarios.first
        
        scenarios.each do |scenario|
          cumulative += scenario[:weight].as(Int32)
          if random_weight < cumulative
            selected_scenario = scenario
            break
          end
        end
        
        scenario_name = selected_scenario[:name].as(String)
        workflow_class = selected_scenario[:workflow]
        
        # Track attempt
        scenario_stats[scenario_name]["attempted"] += 1
        total_workflows += 1
        
        # Execute workflow with error handling
        begin
          workflow_id = "sim-#{scenario_name}-#{iteration}"
          
          case scenario_name
          when "simple_completion"
            result = client.execute_workflow(
              workflow_class.workflow_name,
              "Simulation",
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            
          when "timer_workflow"
            result = client.execute_workflow(
              workflow_class.workflow_name,
              500_i64,
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            
          when "signal_workflow"
            handle = client.start_workflow(
              workflow_class.workflow_name,
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            sleep 100.milliseconds
            3.times { |i| handle.signal("add_value", "val#{i}") }
            handle.signal("finish")
            result = handle.result
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            total_activities += 3
            
          when "activity_workflow"
            result = client.execute_workflow(
              workflow_class.workflow_name,
              3_i64,
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            total_activities += 3
            
          when "multiple_activities"
            result = client.execute_workflow(
              workflow_class.workflow_name,
              3_i64,
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            total_activities += 3
            
          when "cancellation"
            handle = client.start_workflow(
              workflow_class.workflow_name,
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            sleep 100.milliseconds
            handle.cancel
            begin
              handle.result
              scenario_stats[scenario_name]["succeeded"] += 1
              successes += 1
            rescue Temporalio::CancelledError
              scenario_stats[scenario_name]["cancelled"] += 1
              cancellations += 1
            end
            
          when "child_workflow"
            result = client.execute_workflow(
              workflow_class.workflow_name,
              "Child",
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            
          when "continue_as_new"
            result = client.execute_workflow(
              workflow_class.workflow_name,
              0_i64,
              3_i64,
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            scenario_stats[scenario_name]["succeeded"] += 1
            successes += 1
            
          when "activity_timeout"
            begin
              result = client.execute_workflow(
                workflow_class.workflow_name,
                id: workflow_id,
                task_queue: task_queue,
                execution_timeout: 30.seconds
              )
            rescue Temporalio::ActivityError
              scenario_stats[scenario_name]["timeout"] += 1
              timeouts += 1
              total_activities += 1
            end
            
          when "workflow_timeout"
            begin
              result = client.execute_workflow(
                workflow_class.workflow_name,
                id: workflow_id,
                task_queue: task_queue,
                execution_timeout: 2.seconds
              )
            rescue Temporalio::TimeoutError
              scenario_stats[scenario_name]["timeout"] += 1
              timeouts += 1
            end
            
          when "non_retryable"
            begin
              result = client.execute_workflow(
                workflow_class.workflow_name,
                id: workflow_id,
                task_queue: task_queue,
                execution_timeout: 30.seconds
              )
            rescue Temporalio::ActivityError
              scenario_stats[scenario_name]["failed"] += 1
              failures += 1
              total_activities += 1
            end
          end
          
        rescue ex : Exception
          # Any other error
          scenario_stats[scenario_name]["failed"] += 1
          failures += 1
          puts "  [ERROR] #{scenario_name}: #{ex.message}"
        end
        
        # Progress report every 30 seconds
        if (Time.utc - last_report).total_seconds >= 30
          elapsed = (Time.utc - start_time).total_seconds
          remaining = (end_time - Time.utc).total_seconds
          
          puts "\n" + "-"*80
          puts "PROGRESS REPORT (#{elapsed.to_i}s elapsed, #{remaining.to_i}s remaining)"
          puts "-"*80
          puts "Workflows: #{total_workflows} | Activities: #{total_activities}"
          puts "Success: #{successes} | Failed: #{failures} | Timeout: #{timeouts} | Cancelled: #{cancellations}"
          puts "Success Rate: #{(successes.to_f / total_workflows * 100).round(2)}%" if total_workflows > 0
          puts "-"*80 + "\n"
          
          last_report = Time.utc
        end
        
        # Small delay between workflows
        sleep 50.milliseconds
      end
      
    ensure
      puts "\nShutting down worker..."
      worker.initiate_shutdown
      worker.wait_all_complete
      puts "Worker shutdown complete."
    end
    
    # Final results
    elapsed = (Time.utc - start_time).total_seconds
    
    puts "\n" + "="*80
    puts "SIMULATION COMPLETE"
    puts "="*80
    puts "Total Duration: #{elapsed.round(2)}s"
    puts "Workflows Executed: #{total_workflows}"
    puts "Activities Executed: #{total_activities}"
    puts ""
    puts "Results:"
    puts "  ✓ Succeeded: #{successes}"
    puts "  ✗ Failed: #{failures}"
    puts "  ⏱ Timed Out: #{timeouts}"
    puts "  ⊗ Cancelled: #{cancellations}"
    puts ""
    puts "Success Rate: #{(successes.to_f / total_workflows * 100).round(2)}%" if total_workflows > 0
    puts ""
    puts "Throughput:"
    puts "  Workflows: #{(total_workflows / elapsed * 60).round(2)}/minute"
    puts "  Activities: #{(total_activities / elapsed * 60).round(2)}/minute"
    puts ""
    puts "="*80
    puts "BREAKDOWN BY SCENARIO"
    puts "="*80
    
    scenario_stats.each do |name, stats|
      if stats["attempted"] > 0
        puts "\n#{name}:"
        puts "  Attempted: #{stats["attempted"]}"
        puts "  Succeeded: #{stats["succeeded"]}"
        puts "  Failed: #{stats["failed"]}"
        puts "  Timed Out: #{stats["timeout"]}"
        puts "  Cancelled: #{stats["cancelled"]}"
        success_rate = stats["succeeded"].to_f / stats["attempted"] * 100
        puts "  Success Rate: #{success_rate.round(2)}%"
      end
    end
    
    puts "\n" + "="*80
    puts "END OF SIMULATION"
    puts "="*80 + "\n"
    
    # Assertions
    total_workflows.should be > 0
    (successes + failures + timeouts + cancellations).should eq(total_workflows)
  end
end
