require "../simulation_config"

# Main simulation activity that can succeed or fail based on configuration
class SimulationActivity
  include Temporalio::Activity
  
  activity_name "SimulationActivity"
  
  def execute(task_name : String, failure_rate : Float64) : String
    # Simulate work
    sleep Random.rand(100..500).milliseconds

    # Randomly fail based on failure_rate
    if Random.rand < failure_rate
      raise Temporalio::ApplicationError.new(
        "Simulated failure for #{task_name}",
        type: "SimulationFailure",
        non_retryable: false
      )
    end

    "completed:#{task_name}:#{activity.info.attempt}"
  end
end

# Activity that fails until a specific attempt
class FlakeyActivity
  include Temporalio::Activity
  
  activity_name "FlakeyActivity"
  
  @@attempt_counter = Hash(String, Int32).new(0)
  
  def execute(succeed_on_attempt : Int64) : String
    activity_id = activity.info.activity_id
    
    @@attempt_counter[activity_id] ||= 0
    @@attempt_counter[activity_id] += 1
    current_attempt = @@attempt_counter[activity_id]
    
    if current_attempt < succeed_on_attempt
      raise Temporalio::ApplicationError.new(
        "Attempt #{current_attempt}, will succeed on #{succeed_on_attempt}",
        type: "FlakeyFailure",
        non_retryable: false
      )
    end
    
    @@attempt_counter.delete(activity_id)
    "succeeded_on_attempt_#{current_attempt}"
  end
end

# Activity that times out if sleep exceeds timeout
class TimeoutActivity
  include Temporalio::Activity
  
  activity_name "TimeoutActivity"
  
  def execute(sleep_ms : Int64) : String
    # Sleep longer than timeout to trigger timeout
    sleep sleep_ms.milliseconds
    "should_not_reach_here"
  end
end

# Activity with heartbeat support
class HeartbeatActivity
  include Temporalio::Activity
  
  activity_name "HeartbeatActivity"
  
  def execute(iterations : Int64, heartbeat_interval_ms : Int64) : String
    iterations.times do |i|
      activity.heartbeat("iteration_#{i}")

      # Check for cancellation
      activity.check_cancellation!

      sleep heartbeat_interval_ms.milliseconds
    end
    
    "completed_#{iterations}_iterations"
  end
end

# CPU-intensive activity for testing performance
class ComputeActivity
  include Temporalio::Activity
  
  activity_name "ComputeActivity"
  
  def execute(complexity : Int64) : String
    # Simulate CPU work
    result = 0_i64
    complexity.times do |i|
      result += fibonacci(i % 20)
    end
    
    "computed:result=#{result}"
  end
  
  private def fibonacci(n : Int32) : Int64
    return n.to_i64 if n <= 1
    fibonacci(n - 1) + fibonacci(n - 2)
  end
end

# Activity that demonstrates proper error handling
class ValidationActivity
  include Temporalio::Activity
  
  activity_name "ValidationActivity"
  
  def execute(data : String, strict : Bool) : String
    # Validate input
    if data.empty?
      raise Temporalio::ApplicationError.new(
        "Empty data not allowed",
        type: "ValidationError",
        non_retryable: strict
      )
    end
    
    if data.size < 3 && strict
      raise Temporalio::ApplicationError.new(
        "Data too short (min 3 chars)",
        type: "ValidationError",
        non_retryable: true
      )
    end
    
    "validated:#{data}"
  end
end

# Activity that can be cancelled mid-execution
class CancellableActivity
  include Temporalio::Activity
  
  activity_name "CancellableActivity"
  
  def execute(steps : Int64) : String
    steps.times do |i|
      # Check for cancellation before each step
      activity.check_cancellation!

      sleep 100.milliseconds
      activity.heartbeat("step_#{i + 1}")
    end
    
    "completed_all_#{steps}_steps"
  end
end

# Activity that uses worker shutdown detection
class ShutdownAwareActivity
  include Temporalio::Activity
  
  activity_name "ShutdownAwareActivity"
  
  def execute(iterations : Int64) : String
    iterations.times do |i|
      # Check for worker shutdown
      if activity.worker_shutdown?
        return "shutdown_detected_at_iteration_#{i}"
      end
      
      sleep 100.milliseconds
    end
    
    "completed_normally"
  end
end

# Activity demonstrating side effects and idempotency
class IdempotentActivity
  include Temporalio::Activity
  
  activity_name "IdempotentActivity"
  
  @@execution_log = Hash(String, Int32).new(0)
  
  def execute(operation_id : String) : String
    # Track executions to verify idempotency
    @@execution_log[operation_id] ||= 0
    @@execution_log[operation_id] += 1
    
    execution_count = @@execution_log[operation_id]
    
    # Simulate side effect (should only happen once)
    if execution_count == 1
      # First execution - perform side effect
      sleep 200.milliseconds
    end
    
    "operation:#{operation_id}:execution_#{execution_count}"
  end
  
  def self.reset_log
    @@execution_log.clear
  end
end

# Activity that tests different failure modes
class FailureModeActivity
  include Temporalio::Activity
  
  activity_name "FailureModeActivity"
  
  def execute(mode : String) : String
    case mode
    when "timeout"
      sleep 30.seconds # Will timeout
      "should_not_reach"
    when "retryable"
      raise Temporalio::ApplicationError.new(
        "Retryable error",
        type: "RetryableError",
        non_retryable: false
      )
    when "non_retryable"
      raise Temporalio::ApplicationError.new(
        "Non-retryable error",
        type: "NonRetryableError",
        non_retryable: true
      )
    when "crash"
      raise Exception.new("Unexpected crash")
    when "success"
      "success:#{mode}"
    else
      "unknown_mode:#{mode}"
    end
  end
end
