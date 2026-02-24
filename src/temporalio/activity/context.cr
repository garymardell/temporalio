require "./activity_info"

class Fiber
  property temporalio_activity_context : Temporalio::Activity::Context?
end

module Temporalio
  module Activity
    # Describes the reason why an activity's cancellation was requested.
    struct ActivityCancellationDetails
      # True if the workflow explicitly requested cancellation of this activity.
      getter? cancel_requested : Bool
      # True if the activity was not found by the server (e.g. activity ID mismatch).
      getter? not_found : Bool
      # True if the activity was paused.
      getter? paused : Bool
      # True if the activity was reset.
      getter? reset : Bool
      # True if the activity timed out.
      getter? timed_out : Bool
      # True if the worker is shutting down.
      getter? worker_shutdown : Bool

      def initialize(
        @cancel_requested : Bool = false,
        @not_found : Bool = false,
        @paused : Bool = false,
        @reset : Bool = false,
        @timed_out : Bool = false,
        @worker_shutdown : Bool = false
      )
      end
    end

    # Per-activity execution context. Stored directly on the current fiber.
    # Access the current context via Context.current inside an activity.
    class Context
      # Returns the current activity context for the calling fiber.
      # Raises if called outside an activity execution.
      def self.current : Context
        ctx = Fiber.current.temporalio_activity_context
        raise Error.new("No activity context — called outside an activity execution") unless ctx
        ctx
      end

      # Returns the current activity context or nil if not in an activity.
      def self.current? : Context?
        Fiber.current.temporalio_activity_context
      end

      getter info : ActivityInfo

      @cancel_requested : Bool = false
      @worker_shutdown : Bool = false
      @heartbeat_proc : Proc(Array(Temporal::Api::Common::V1::Payload), Nil)?
      @cancellation_cause : Symbol = :cancel_requested

      def initialize(
        @info : ActivityInfo,
        @heartbeat_proc : Proc(Array(Temporal::Api::Common::V1::Payload), Nil)? = nil
      )
      end

      # Send a heartbeat with any JSON-serializable detail values.
      # Each detail is encoded to a Payload. Pass strings, integers, booleans,
      # or any object including JSON::Serializable.
      # Raises CancelledError if cancellation has been requested.
      def heartbeat(*details) : Nil
        dc = Temporalio::DataConverter::DEFAULT
        payloads = details.to_a.map { |d| dc.to_payload(d).as(Temporal::Api::Common::V1::Payload) }
        @heartbeat_proc.try(&.call(payloads))
        check_cancellation! if @cancel_requested
      end

      # Check if cancellation has been requested. Raises CancelledError if so.
      def check_cancellation! : Nil
        raise CancelledError.new("Activity cancelled") if @cancel_requested
      end

      # Returns true if the activity has been requested to cancel.
      def cancelled? : Bool
        @cancel_requested
      end

      # Returns true if the worker is shutting down.
      def worker_shutdown? : Bool
        @worker_shutdown
      end

      # Returns details about why cancellation was requested, or nil if not cancelled.
      def cancellation_details : ActivityCancellationDetails?
        return nil unless @cancel_requested
        ActivityCancellationDetails.new(
          cancel_requested: @cancellation_cause == :cancel_requested,
          not_found: @cancellation_cause == :not_found,
          paused: @cancellation_cause == :paused,
          reset: @cancellation_cause == :reset,
          timed_out: @cancellation_cause == :timed_out,
          worker_shutdown: @cancellation_cause == :worker_shutdown
        )
      end

      # Internal: called when a cancel task arrives.
      def request_cancel! : Nil
        @cancel_requested = true
        @cancellation_cause = :cancel_requested
      end

      # Internal: called when worker initiates shutdown.
      def notify_worker_shutdown! : Nil
        @worker_shutdown = true
        @cancel_requested = true
        @cancellation_cause = :worker_shutdown
      end

      # Internal: register this context for the current fiber.
      def install! : Nil
        Fiber.current.temporalio_activity_context = self
      end

      # Internal: unregister this context from the current fiber.
      def uninstall! : Nil
        Fiber.current.temporalio_activity_context = nil
      end
    end
  end
end
