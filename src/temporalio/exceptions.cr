require "json"

module Temporalio
  # Base error for all Temporalio SDK errors.
  class Error < Exception
  end

  # Base class for errors that represent workflow/activity failures that have
  # a corresponding proto Failure message.
  class FailureError < Error
    def initialize(message : String, cause : Exception? = nil)
      super(message, cause)
    end
  end

  # Raised when an RPC call to Temporal server fails.
  class RPCError < Error
    getter grpc_status : Int32
    getter details : Bytes?

    def initialize(message : String, @grpc_status : Int32, @details : Bytes? = nil)
      super(message)
    end

    def status_name : String
      case @grpc_status
      when 1  then "CANCELLED"
      when 2  then "UNKNOWN"
      when 3  then "INVALID_ARGUMENT"
      when 4  then "DEADLINE_EXCEEDED"
      when 5  then "NOT_FOUND"
      when 6  then "ALREADY_EXISTS"
      when 7  then "PERMISSION_DENIED"
      when 8  then "RESOURCE_EXHAUSTED"
      when 9  then "FAILED_PRECONDITION"
      when 10 then "ABORTED"
      when 11 then "OUT_OF_RANGE"
      when 12 then "UNIMPLEMENTED"
      when 13 then "INTERNAL"
      when 14 then "UNAVAILABLE"
      when 15 then "DATA_LOSS"
      when 16 then "UNAUTHENTICATED"
      else         "UNKNOWN(#{@grpc_status})"
      end
    end
  end

  # Raised when attempting to start a workflow that is already running or
  # was already started with the same workflow_id.
  class WorkflowAlreadyStartedError < RPCError
    getter workflow_id : String
    getter run_id : String?

    def initialize(@workflow_id : String, @run_id : String? = nil)
      super("Workflow already started: #{workflow_id}", grpc_status: 6)
    end
  end

  # Raised when a workflow or run is not found.
  class WorkflowNotFoundError < RPCError
    def initialize(message : String = "Workflow not found")
      super(message, grpc_status: 5)
    end
  end

  # Raised when the workflow code violates determinism constraints.
  class NondeterminismError < Error
  end

  # Represents an application-level failure — user code threw an exception.
  class ApplicationError < FailureError
    getter type : String?
    getter non_retryable : Bool
    # Raw JSON strings for each detail payload. Decode with your type: T.from_json(detail).
    getter details : Array(String)
    getter next_retry_delay : Time::Span?

    def initialize(
      message : String,
      @type : String? = nil,
      @non_retryable : Bool = false,
      @details : Array(String) = [] of String,
      @next_retry_delay : Time::Span? = nil,
      cause : Exception? = nil
    )
      super(message, cause)
    end
  end

  # Raised when a workflow or activity is cancelled.
  class CancelledError < FailureError
    # Raw JSON strings for each detail payload.
    getter details : Array(String)

    def initialize(
      message : String = "Cancelled",
      @details : Array(String) = [] of String,
      cause : Exception? = nil
    )
      super(message, cause)
    end
  end

  # Raised when a workflow is terminated.
  class TerminatedError < FailureError
    # Raw JSON strings for each detail payload.
    getter details : Array(String)

    def initialize(
      message : String = "Terminated",
      @details : Array(String) = [] of String,
      cause : Exception? = nil
    )
      super(message, cause)
    end
  end

  # Raised when a workflow or activity times out.
  class TimeoutError < FailureError
    getter timeout_type : Int32
    # Raw JSON strings for each heartbeat detail payload.
    getter last_heartbeat_details : Array(String)

    def initialize(
      message : String,
      @timeout_type : Int32,
      @last_heartbeat_details : Array(String) = [] of String,
      cause : Exception? = nil
    )
      super(message, cause)
    end

    def timeout_type_name : String
      case @timeout_type
      when 1 then "START_TO_CLOSE"
      when 2 then "SCHEDULE_TO_START"
      when 3 then "SCHEDULE_TO_CLOSE"
      when 4 then "HEARTBEAT"
      else        "UNKNOWN(#{@timeout_type})"
      end
    end
  end

  # Raised for server-side failures.
  class ServerError < FailureError
    getter non_retryable : Bool

    def initialize(
      message : String,
      @non_retryable : Bool = false,
      cause : Exception? = nil
    )
      super(message, cause)
    end
  end

  # Wraps a failure that occurred within an activity execution.
  class ActivityError < FailureError
    getter scheduled_event_id : Int64
    getter started_event_id : Int64
    getter activity_type : String
    getter activity_id : String
    getter retry_state : Int32

    def initialize(
      message : String,
      @scheduled_event_id : Int64,
      @started_event_id : Int64,
      @activity_type : String,
      @activity_id : String,
      @retry_state : Int32,
      cause : Exception? = nil
    )
      super(message, cause)
    end
  end

  # Wraps a failure that occurred within a child workflow execution.
  class ChildWorkflowError < FailureError
    getter namespace : String
    getter workflow_id : String
    getter run_id : String
    getter workflow_type : String
    getter retry_state : Int32

    def initialize(
      message : String,
      @namespace : String,
      @workflow_id : String,
      @run_id : String,
      @workflow_type : String,
      @retry_state : Int32,
      cause : Exception? = nil
    )
      super(message, cause)
    end
  end
end
