require "json"

# Configuration for simulation run
class SimulationConfig
  include JSON::Serializable
  
  property duration : Time::Span
  property num_workflows : Int32
  property activities_per_workflow : Int32
  property failure_rate : Float64
  property timeout_rate : Float64
  property worker_restart_interval : Time::Span
  property enable_child_workflows : Bool
  property enable_continue_as_new : Bool
  property enable_signals : Bool
  property enable_queries : Bool
  property enable_cancellation : Bool
  property max_workflow_depth : Int32
  
  def initialize(
    @duration = 60.seconds,
    @num_workflows = 10,
    @activities_per_workflow = 3,
    @failure_rate = 0.1,
    @timeout_rate = 0.05,
    @worker_restart_interval = 30.seconds,
    @enable_child_workflows = true,
    @enable_continue_as_new = true,
    @enable_signals = true,
    @enable_queries = true,
    @enable_cancellation = true,
    @max_workflow_depth = 2
  )
  end
end

# Metrics collected during simulation
class SimulationMetrics
  include JSON::Serializable
  
  property duration_seconds : Float64
  property total_workflows : Int32
  property total_activities : Int32
  property success_rate : Float64
  property scenario_results : Hash(String, ScenarioStats)
  property replay_errors : Int32
  property nondeterministic_events : Int32
  property worker_restarts : Int32
  property activity_retries : Int32
  property workflow_cancellations : Int32
  
  def initialize
    @duration_seconds = 0.0
    @total_workflows = 0
    @total_activities = 0
    @success_rate = 0.0
    @scenario_results = Hash(String, ScenarioStats).new
    @replay_errors = 0
    @nondeterministic_events = 0
    @worker_restarts = 0
    @activity_retries = 0
    @workflow_cancellations = 0
  end
end

# Statistics for a specific scenario
class ScenarioStats
  include JSON::Serializable
  
  property executed : Int32
  property succeeded : Int32
  property failed : Int32
  property timed_out : Int32
  property cancelled : Int32
  property avg_duration_ms : Float64
  
  def initialize
    @executed = 0
    @succeeded = 0
    @failed = 0
    @timed_out = 0
    @cancelled = 0
    @avg_duration_ms = 0.0
  end
  
  def record_success(duration_ms : Float64)
    @executed += 1
    @succeeded += 1
    update_avg_duration(duration_ms)
  end
  
  def record_failure
    @executed += 1
    @failed += 1
  end
  
  def record_timeout
    @executed += 1
    @timed_out += 1
  end
  
  def record_cancellation
    @executed += 1
    @cancelled += 1
  end
  
  private def update_avg_duration(duration_ms : Float64)
    total = @avg_duration_ms * (@succeeded - 1) + duration_ms
    @avg_duration_ms = total / @succeeded
  end
end
