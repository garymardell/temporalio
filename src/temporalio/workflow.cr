require "./internal/proto"
require "./data_converter"
require "./interceptor/worker_interceptor"

class Fiber
  property temporalio_workflow_context : Temporalio::Workflow::Context?
end

module Temporalio
  module Workflow
    # Controls how a running activity reacts when the workflow requests cancellation.
    enum ActivityCancellationType
      # Request cancellation and move on immediately (default).
      TRY_CANCEL = 0
      # Wait for the activity to acknowledge cancellation before continuing.
      WAIT_CANCELLATION_COMPLETED = 1
      # Don't request cancellation at all — let the activity run to completion.
      ABANDON = 2
    end

    # Controls how a running child workflow reacts when the parent requests cancellation.
    enum ChildWorkflowCancellationType
      ABANDON = 0
      TRY_CANCEL = 1
      WAIT_CANCELLATION_COMPLETED = 2
      WAIT_CANCELLATION_REQUESTED = 3
    end

    # Controls what happens to a child workflow when its parent workflow closes.
    enum ParentClosePolicy
      UNSPECIFIED = 0
      TERMINATE = 1
      ABANDON = 2
      REQUEST_CANCEL = 3
    end
  end

  # Raised by Context#continue_as_new to signal the workflow runner.
  class ContinueAsNewError < Error
    getter workflow_type : String?
    getter args : Array(Temporal::Api::Common::V1::Payload)
    getter task_queue : String?
    getter execution_timeout : Time::Span?
    getter run_timeout : Time::Span?
    getter task_timeout : Time::Span?
    getter retry_policy : Client::RetryPolicy?
    getter memo : Hash(String, String)?
    getter search_attributes : Hash(String, String)?

    def initialize(
      @args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
      @workflow_type : String? = nil,
      @task_queue : String? = nil,
      @execution_timeout : Time::Span? = nil,
      @run_timeout : Time::Span? = nil,
      @task_timeout : Time::Span? = nil,
      @retry_policy : Client::RetryPolicy? = nil,
      @memo : Hash(String, String)? = nil,
      @search_attributes : Hash(String, String)? = nil
    )
      super("ContinueAsNew")
    end
  end

  # Mixin module for defining Temporal workflows.
  #
  # Usage:
  #   class MyWorkflow
  #     include Temporalio::Workflow
  #
  #     workflow_name "MyWorkflow"  # optional — defaults to class name
  #
  #     def execute(name : String) : String
  #       "Hello, #{name}!"
  #     end
  #     workflow_dispatch
  #
  #     workflow_signal("my-signal") do |value : String|
  #       @received = value
  #     end
  #
  #     workflow_query("state") do : String
  #       @received
  #     end
  #   end
  module Workflow
    macro included
      # The Temporal workflow type name. Defaults to class name.
      def self.workflow_name : String
        {{@type.name.stringify}}
      end

      # Returns the current workflow context. Available inside any workflow method.
      def workflow : Temporalio::Workflow::Context
        Temporalio::Workflow::Context.current
      end

      # Override the workflow type name registered with Temporal.
      macro workflow_name(wf_name)
        def self.workflow_name : String
          \{{wf_name}}
        end
      end

      # Signal handler registry: signal_name => Proc (legacy, for manual registration)
      {% unless @type.has_constant?("SIGNAL_HANDLERS") %}
        SIGNAL_HANDLERS = {} of String => Proc(Array(Temporal::Api::Common::V1::Payload), Nil)
      {% end %}

      # Query handler registry: query_name => Proc (legacy, for manual registration)
      {% unless @type.has_constant?("QUERY_HANDLERS") %}
        QUERY_HANDLERS = {} of String => Proc(Array(Temporal::Api::Common::V1::Payload), Temporal::Api::Common::V1::Payload?)
      {% end %}

      # Macro-registered signal names (accumulated by workflow_signal calls).
      {% unless @type.has_constant?("REGISTERED_SIGNALS") %}
        REGISTERED_SIGNALS = [] of String
      {% end %}

      # Macro-registered query names (accumulated by workflow_query calls).
      {% unless @type.has_constant?("REGISTERED_QUERIES") %}
        REGISTERED_QUERIES = [] of String
      {% end %}

      # Macro-registered update names (accumulated by workflow_update calls).
      {% unless @type.has_constant?("REGISTERED_UPDATES") %}
        REGISTERED_UPDATES = [] of String
      {% end %}

      # Update validators registry (update_name => true if validator exists).
      {% unless @type.has_constant?("UPDATE_VALIDATORS") %}
        UPDATE_VALIDATORS = {} of String => Bool
      {% end %}

      # Whether a dynamic (catch-all) signal handler is registered.
      {% unless @type.has_constant?("HAS_DYNAMIC_SIGNAL") %}
        HAS_DYNAMIC_SIGNAL = false
      {% end %}

      # Whether a dynamic (catch-all) query handler is registered.
      {% unless @type.has_constant?("HAS_DYNAMIC_QUERY") %}
        HAS_DYNAMIC_QUERY = false
      {% end %}

      # Whether a dynamic (catch-all) update handler is registered.
      {% unless @type.has_constant?("HAS_DYNAMIC_UPDATE") %}
        HAS_DYNAMIC_UPDATE = false
      {% end %}

      # The finished hook auto-generates _temporal_execute after the class body completes.
      # This introspects the execute method and creates the typed dispatch automatically.
      macro finished
        \{% method = @type.methods.find { |m| m.name == "execute" } %}
        \{% if method %}
          def _temporal_execute(
            payloads : Array(Temporal::Api::Common::V1::Payload),
            converter : Temporalio::DataConverter
          ) : Temporal::Api::Common::V1::Payload?
            \{% args = method.args %}
            \{% if args.size == 0 %}
              result = execute
            \{% else %}
              \{% for arg, idx in args %}
                _arg\{{idx}} = converter.from_payload(payloads[\{{idx}}], \{{arg.restriction}})
              \{% end %}
              result = execute(\{% for arg, idx in args %}_arg\{{idx}}\{% if idx < args.size - 1 %}, \{% end %}\{% end %})
            \{% end %}
            converter.to_payload(result)
          end
        \{% else %}
          def _temporal_execute(
            payloads : Array(Temporal::Api::Common::V1::Payload),
            converter : Temporalio::DataConverter
          ) : Temporal::Api::Common::V1::Payload?
            raise Temporalio::Error.new("#{self.class}: no execute method found")
          end
        \{% end %}
      end

      # Register a signal handler on this workflow class.
      #
      # Usage (no args):
      #   workflow_signal("trigger") do
      #     @triggered = true
      #   end
      #
      # Usage (with typed args — pass types after signal name, vars in block):
      #   workflow_signal("my-signal", String) do |value|
      #     @state = value
      #   end
      #   workflow_signal("add", String, Int64) do |name, count|
      #     @name = name; @count = count
      #   end
      #
      # Multiple workflow_signal calls accumulate — all are dispatched correctly.
      # Crystal macro blocks don't support typed params (|v : T|), so pass
      # types as positional arguments after the signal name instead.
      macro workflow_signal(signal_name, *arg_types, &block)
        \{% params = block.args %}
        \{% body = block.body %}
        \{% safe_name = signal_name.gsub(/-/, "_").id %}

        def _temporal_signal_handler_\{{safe_name}}(
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Nil
          \{% if arg_types.size == 0 || params.size == 0 %}
            \{{body}}
          \{% elsif arg_types.size >= 1 && params.size >= 1 %}
            \{{params[0]}} = converter.from_payload(payloads[0], \{{arg_types[0]}})
            \{% if arg_types.size >= 2 && params.size >= 2 %}
              \{{params[1]}} = converter.from_payload(payloads[1], \{{arg_types[1]}})
              \{% if arg_types.size >= 3 && params.size >= 3 %}
                \{{params[2]}} = converter.from_payload(payloads[2], \{{arg_types[2]}})
              \{% end %}
            \{% end %}
            \{{body}}
          \{% end %}
        end

        \{% REGISTERED_SIGNALS << signal_name %}

        # Redefine _temporal_handle_signal to dispatch all registered signals.
        def _temporal_handle_signal(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Nil
          case name
          \{% for sn in REGISTERED_SIGNALS %}
          when \{{sn}}
            _temporal_signal_handler_\{{sn.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            # Unknown signal — silently ignore (Temporal best practice)
          end
        end
      end

      # Register a query handler on this workflow class.
      #
      # Usage (no args):
      #   workflow_query("get-state", return_type: String) do
      #     @state
      #   end
      #
      # Usage (with typed args — pass types after query name, vars in block):
      #   workflow_query("multiply", Int64, return_type: Int64) do |factor|
      #     @value * factor
      #   end
      #
      # Multiple workflow_query calls accumulate — all are dispatched correctly.
      macro workflow_query(query_name, *arg_types, return_type _rt = Nil, &block)
        \{% params = block.args %}
        \{% body = block.body %}
        \{% safe_name = query_name.gsub(/-/, "_").id %}

        def _temporal_query_handler_\{{safe_name}}(
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          \{% if arg_types.size == 0 || params.size == 0 %}
            result = begin \{{body}} end
          \{% elsif arg_types.size >= 1 && params.size >= 1 %}
            \{{params[0]}} = converter.from_payload(payloads[0], \{{arg_types[0]}})
            \{% if arg_types.size >= 2 && params.size >= 2 %}
              \{{params[1]}} = converter.from_payload(payloads[1], \{{arg_types[1]}})
            \{% end %}
            result = begin \{{body}} end
          \{% end %}
          converter.to_payload(result)
        end

        \{% REGISTERED_QUERIES << query_name %}

        def _temporal_handle_query(
          query_id : String,
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          case name
          \{% for qn in REGISTERED_QUERIES %}
          when \{{qn}}
            _temporal_query_handler_\{{qn.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            nil
          end
        end
      end

      # Register an update handler on this workflow class.
      #
      # Usage (no args):
      #   workflow_update("increment") do
      #     @count += 1
      #     @count
      #   end
      #
      # Usage (with typed args):
      #   workflow_update("add", Int64) do |amount|
      #     @count += amount
      #     @count
      #   end
      #
      # Usage (with validator):
      #   workflow_update("withdraw", Int64, validator: true) do |amount|
      #     @balance -= amount
      #     @balance
      #   end
      #
      #   workflow_update_validator("withdraw", Int64) do |amount|
      #     raise "Insufficient funds" if amount > @balance
      #   end
      #
      # Multiple workflow_update calls accumulate — all are dispatched correctly.
      macro workflow_update(update_name, *arg_types, validator = false, &block)
        \{% params = block.args %}
        \{% body = block.body %}
        \{% safe_name = update_name.gsub(/-/, "_").id %}

        def _temporal_update_handler_\{{safe_name}}(
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          \{% if arg_types.size == 0 || params.size == 0 %}
            result = begin \{{body}} end
          \{% elsif arg_types.size >= 1 && params.size >= 1 %}
            \{{params[0]}} = converter.from_payload(payloads[0], \{{arg_types[0]}})
            \{% if arg_types.size >= 2 && params.size >= 2 %}
              \{{params[1]}} = converter.from_payload(payloads[1], \{{arg_types[1]}})
              \{% if arg_types.size >= 3 && params.size >= 3 %}
                \{{params[2]}} = converter.from_payload(payloads[2], \{{arg_types[2]}})
              \{% end %}
            \{% end %}
            result = begin \{{body}} end
          \{% end %}
          converter.to_payload(result)
        end

        \{% REGISTERED_UPDATES << update_name %}

        def _temporal_handle_update(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          case name
          \{% for un in REGISTERED_UPDATES %}
          when \{{un}}
            _temporal_update_handler_\{{un.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            nil
          end
        end
      end

      # Register a validator for an update handler.
      #
      # Usage:
      #   workflow_update_validator("withdraw", Int64) do |amount|
      #     raise "Insufficient funds" if amount > @balance
      #   end
      #
      # Validators run synchronously before the update is accepted.
      # They MUST NOT yield or perform any workflow operations.
      # If the validator raises an exception, the update is rejected.
      macro workflow_update_validator(update_name, *arg_types, &block)
        \{% params = block.args %}
        \{% body = block.body %}
        \{% safe_name = update_name.gsub(/-/, "_").id %}

        def _temporal_update_validator_\{{safe_name}}(
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Nil
          \{% if arg_types.size == 0 || params.size == 0 %}
            \{{body}}
          \{% elsif arg_types.size >= 1 && params.size >= 1 %}
            \{{params[0]}} = converter.from_payload(payloads[0], \{{arg_types[0]}})
            \{% if arg_types.size >= 2 && params.size >= 2 %}
              \{{params[1]}} = converter.from_payload(payloads[1], \{{arg_types[1]}})
              \{% if arg_types.size >= 3 && params.size >= 3 %}
                \{{params[2]}} = converter.from_payload(payloads[2], \{{arg_types[2]}})
              \{% end %}
            \{% end %}
            \{{body}}
          \{% end %}
          # If no exception raised, validation passes
          nil
        end

        \{% UPDATE_VALIDATORS[update_name] = true %}

        def _temporal_handle_update_validator(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Nil
          case name
          \{% for un, _ in UPDATE_VALIDATORS %}
          when \{{un}}
            _temporal_update_validator_\{{un.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            # No validator - auto-accept
          end
        end
      end

      # Register a dynamic (catch-all) signal handler.
      # Called for any signal whose name is not matched by a workflow_signal macro.
      # The block receives (name : String, payloads : Array(Payload)).
      #
      # Usage:
      #   workflow_dynamic_signal do |name, payloads|
      #     @received[name] = payloads
      #   end
      macro workflow_dynamic_signal(&block)
        \{% params = block.args %}
        \{% body = block.body %}

        def _temporal_dynamic_signal_handler(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Nil
          \{% if params.size >= 2 %}
            \{{params[0]}} = name
            \{{params[1]}} = payloads
          \{% elsif params.size == 1 %}
            \{{params[0]}} = name
          \{% end %}
          \{{body}}
        end

        \{% HAS_DYNAMIC_SIGNAL = true %}

        def _temporal_handle_signal(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Nil
          case name
          \{% for sn in REGISTERED_SIGNALS %}
          when \{{sn}}
            _temporal_signal_handler_\{{sn.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            _temporal_dynamic_signal_handler(name, payloads, converter)
          end
        end
      end

      # Register a dynamic (catch-all) query handler.
      # Called for any query whose name is not matched by a workflow_query macro.
      # The block receives (name : String, payloads : Array(Payload)) and must return a payload.
      #
      # Usage:
      #   workflow_dynamic_query do |name, payloads| : String
      #     "unknown query: #{name}"
      #   end
      macro workflow_dynamic_query(&block)
        \{% params = block.args %}
        \{% body = block.body %}

        def _temporal_dynamic_query_handler(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          \{% if params.size >= 2 %}
            \{{params[0]}} = name
            \{{params[1]}} = payloads
          \{% elsif params.size == 1 %}
            \{{params[0]}} = name
          \{% end %}
          result = begin \{{body}} end
          converter.to_payload(result)
        end

        \{% HAS_DYNAMIC_QUERY = true %}

        def _temporal_handle_query(
          query_id : String,
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          case name
          \{% for qn in REGISTERED_QUERIES %}
          when \{{qn}}
            _temporal_query_handler_\{{qn.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            _temporal_dynamic_query_handler(name, payloads, converter)
          end
        end
      end

      # Register a dynamic (catch-all) update handler.
      # Called for any update whose name is not matched by a workflow_update macro.
      # The block receives (name : String, payloads : Array(Payload)).
      #
      # Usage:
      #   workflow_dynamic_update do |name, payloads|
      #     @events << name
      #   end
      macro workflow_dynamic_update(&block)
        \{% params = block.args %}
        \{% body = block.body %}

        def _temporal_dynamic_update_handler(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          \{% if params.size >= 2 %}
            \{{params[0]}} = name
            \{{params[1]}} = payloads
          \{% elsif params.size == 1 %}
            \{{params[0]}} = name
          \{% end %}
          result = begin \{{body}} end
          converter.to_payload(result)
        end

        \{% HAS_DYNAMIC_UPDATE = true %}

        def _temporal_handle_update(
          name : String,
          payloads : Array(Temporal::Api::Common::V1::Payload),
          converter : Temporalio::DataConverter
        ) : Temporal::Api::Common::V1::Payload?
          case name
          \{% for un in REGISTERED_UPDATES %}
          when \{{un}}
            _temporal_update_handler_\{{un.gsub(/-/, "_").id}}(payloads, converter)
          \{% end %}
          else
            _temporal_dynamic_update_handler(name, payloads, converter)
          end
        end
      end

      # Handle a signal before fiber resume (instance-level dispatch).
      # Default: silently ignore unknown signals.
      # Overridden by workflow_signal macro with typed dispatch.
      def _temporal_handle_signal(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : Temporalio::DataConverter
      ) : Nil
        # No macro-registered handlers — ignore
      end

      # Handle a query in the poller fiber (read-only, no fiber resume).
      # Default: returns nil (query not found).
      # Overridden by workflow_query macro with typed dispatch.
      def _temporal_handle_query(
        query_id : String,
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : Temporalio::DataConverter
      ) : Temporal::Api::Common::V1::Payload?
        nil
      end

      # Handle an update (runs asynchronously in workflow fiber).
      # Default: returns nil (update not found).
      # Overridden by workflow_update macro with typed dispatch.
      def _temporal_handle_update(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : Temporalio::DataConverter
      ) : Temporal::Api::Common::V1::Payload?
        nil
      end

      # Handle an update validator (runs synchronously in poller fiber).
      # Default: auto-accepts (no validation).
      # Overridden by workflow_update_validator macro with typed dispatch.
      def _temporal_handle_update_validator(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : Temporalio::DataConverter
      ) : Nil
        # No validator - auto-accept
      end
    end

    # The workflow context, available inside workflow code via Workflow::Context.current.
    class Context
      # Returns the current workflow context (must be called from within a workflow fiber).
      def self.current : Context
        ctx = Fiber.current.temporalio_workflow_context
        raise Error.new("No workflow context — called outside a workflow fiber") unless ctx
        ctx
      end

      getter run_id : String
      getter workflow_type : String
      getter workflow_id : String
      getter namespace : String
      getter task_queue : String
      getter attempt : Int32
      getter data_converter : DataConverter

      # Deterministic current time (set at start of each activation).
      getter now : Time

      # Wall-clock time when the workflow execution first started.
      getter start_time : Time

      # Parent workflow info if this workflow was started as a child, nil otherwise.
      getter parent : ParentInfo?

      # Run ID of the previous run if this is a continuation via continue-as-new, nil otherwise.
      getter continued_run_id : String?

      # Number of events in the workflow history as of the last task.
      getter history_length : UInt32

      # Byte size of workflow history as of the last task.
      getter history_size_bytes : UInt64

      # True if the server recommends calling continue-as-new (history is getting large).
      getter? continue_as_new_suggested : Bool

      # True if the current activation is replaying history rather than processing new events.
      getter? replaying : Bool

      # Deterministic pseudo-random number generator seeded by the workflow.
      # Use this instead of Random::DEFAULT to keep workflows deterministic.
      getter random : Random

      # Maximum time the entire workflow execution is allowed to run (nil if not set).
      getter execution_timeout : Time::Span?

      # Maximum time for a single run of the workflow (nil if not set).
      getter run_timeout : Time::Span?

      # Maximum time for a single workflow task (nil if not set).
      getter task_timeout : Time::Span?

      # The retry policy configured for this workflow (nil if not set).
      getter retry_policy : Client::RetryPolicy?

      # The cron schedule string if this workflow was started as a cron workflow (nil otherwise).
      getter cron_schedule : String?

      # Memo key/value pairs set when the workflow was started.
      getter memo : Hash(String, Array(Temporal::Api::Common::V1::Payload))

      # Search attributes set on the workflow.
      getter search_attributes : Hash(String, Array(Temporal::Api::Common::V1::Payload))

      # Root workflow execution (the top-level workflow for a chain of child workflows).
      getter root_workflow : RootInfo?

      # First execution run ID (the run ID of the very first run in a continue-as-new chain).
      getter first_execution_run_id : String?

      struct RootInfo
        getter workflow_id : String
        getter run_id : String

        def initialize(@workflow_id, @run_id)
        end
      end

      # Parent workflow reference, populated when this workflow runs as a child.
      struct ParentInfo
        getter namespace : String
        getter workflow_id : String
        getter run_id : String

        def initialize(@namespace, @workflow_id, @run_id)
        end
      end

      # Accumulated commands for the current activation.
      @commands : Array(Coresdk::WorkflowCommands::WorkflowCommand)

      # Pending resolutions keyed by sequence number.
      @pending_timers : Hash(UInt32, Channel(Nil))
      @pending_activities : Hash(UInt32, Channel(Coresdk::ActivityResult::ActivityResolution))
      @pending_child_start : Hash(UInt32, Channel(Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStart))
      @pending_child_complete : Hash(UInt32, Channel(Coresdk::ChildWorkflow::ChildWorkflowResult))
      @pending_external_signals : Hash(UInt32, Channel(Coresdk::WorkflowActivation::ResolveSignalExternalWorkflow))
      @pending_external_cancels : Hash(UInt32, Channel(Coresdk::WorkflowActivation::ResolveRequestCancelExternalWorkflow))
      @next_seq : UInt32

      # Fiber coordination channels.
      @resume_channel : Channel(Nil)
      @suspended_channel : Channel(Nil)

      # Cancellation and patch state.
      @cancelled : Bool = false
      @recorded_patches : Set(String)

      # Registered signal/query handlers (instance-level, set from workflow class).
      @signal_handlers : Hash(String, Proc(Array(Temporal::Api::Common::V1::Payload), Nil))
      @query_handlers : Hash(String, Proc(Array(Temporal::Api::Common::V1::Payload), Temporal::Api::Common::V1::Payload?))

      # Pending updates waiting for handler execution.
      @pending_updates : Hash(String, PendingUpdate)

      # Worker-side interceptors for outbound workflow operations.
      @interceptors : Array(Interceptor::WorkerInterceptor)

      struct PendingUpdate
        property protocol_instance_id : String
        property name : String
        property input : Array(Temporal::Api::Common::V1::Payload)

        def initialize(@protocol_instance_id, @name, @input)
        end
      end

      def initialize(
        @run_id : String,
        @workflow_type : String,
        @workflow_id : String,
        @namespace : String,
        @task_queue : String,
        @attempt : Int32,
        @data_converter : DataConverter,
        @now : Time,
        @start_time : Time,
        @parent : ParentInfo?,
        @continued_run_id : String?,
        @history_length : UInt32,
        @history_size_bytes : UInt64,
        @continue_as_new_suggested : Bool,
        @replaying : Bool,
        random_seed : UInt64,
        @resume_channel : Channel(Nil),
        @suspended_channel : Channel(Nil),
        @execution_timeout : Time::Span? = nil,
        @run_timeout : Time::Span? = nil,
        @task_timeout : Time::Span? = nil,
        @retry_policy : Client::RetryPolicy? = nil,
        @cron_schedule : String? = nil,
        @memo : Hash(String, Array(Temporal::Api::Common::V1::Payload)) = {} of String => Array(Temporal::Api::Common::V1::Payload),
        @search_attributes : Hash(String, Array(Temporal::Api::Common::V1::Payload)) = {} of String => Array(Temporal::Api::Common::V1::Payload),
        @root_workflow : RootInfo? = nil,
        @first_execution_run_id : String? = nil,
        @interceptors : Array(Interceptor::WorkerInterceptor) = [] of Interceptor::WorkerInterceptor
      )
        @random = Random.new(random_seed.to_i64)
        @commands = [] of Coresdk::WorkflowCommands::WorkflowCommand
        @pending_timers = {} of UInt32 => Channel(Nil)
        @pending_activities = {} of UInt32 => Channel(Coresdk::ActivityResult::ActivityResolution)
        @pending_child_start = {} of UInt32 => Channel(Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStart)
        @pending_child_complete = {} of UInt32 => Channel(Coresdk::ChildWorkflow::ChildWorkflowResult)
        @pending_external_signals = {} of UInt32 => Channel(Coresdk::WorkflowActivation::ResolveSignalExternalWorkflow)
        @pending_external_cancels = {} of UInt32 => Channel(Coresdk::WorkflowActivation::ResolveRequestCancelExternalWorkflow)
        @next_seq = 1_u32
        @recorded_patches = Set(String).new
        @signal_handlers = {} of String => Proc(Array(Temporal::Api::Common::V1::Payload), Nil)
        @query_handlers = {} of String => Proc(Array(Temporal::Api::Common::V1::Payload), Temporal::Api::Common::V1::Payload?)
        @pending_updates = {} of String => PendingUpdate
      end

      # Register a signal handler on this context instance.
      def register_signal(name : String, &block : Array(Temporal::Api::Common::V1::Payload) -> Nil) : Nil
        @signal_handlers[name] = block
      end

      # Register a query handler on this context instance.
      def register_query(name : String, &block : Array(Temporal::Api::Common::V1::Payload) -> Temporal::Api::Common::V1::Payload?) : Nil
        @query_handlers[name] = block
      end

      # Update the deterministic time (called at start of each activation).
      def update_time(ts : Google::Protobuf::Timestamp?) : Nil
        return unless ts
        secs = ts.seconds || 0_i64
        nanos = ts.nanos || 0
        @now = Time.unix(secs) + nanos.nanoseconds
      end

      # Update per-activation metadata from the WorkflowActivation proto.
      def update_activation_info(
        history_length : UInt32,
        history_size_bytes : UInt64,
        continue_as_new_suggested : Bool,
        replaying : Bool
      ) : Nil
        @history_length = history_length
        @history_size_bytes = history_size_bytes
        @continue_as_new_suggested = continue_as_new_suggested
        @replaying = replaying
      end

      # Reseed the deterministic RNG (called when an UpdateRandomSeed job arrives).
      def update_random_seed(seed : UInt64) : Nil
        @random = Random.new(seed.to_i64)
      end

      # Collect and clear accumulated commands for the current activation.
      def drain_commands : Array(Coresdk::WorkflowCommands::WorkflowCommand)
        cmds = @commands.dup
        @commands.clear
        cmds
      end

      # Check if there's any pending work (activities, timers, child workflows, external ops).
      def has_pending_work? : Bool
        !@pending_activities.empty? || !@pending_timers.empty? ||
          !@pending_child_start.empty? || !@pending_child_complete.empty? ||
          !@pending_external_signals.empty? || !@pending_external_cancels.empty?
      end

      # Enqueue a command (used by WorkflowInstance for query responses).
      def enqueue_command(cmd : Coresdk::WorkflowCommands::WorkflowCommand) : Nil
        @commands << cmd
      end

      # Handle a signal — dispatch to registered handler. Returns true if handled.
      def handle_signal(name : String, payloads : Array(Temporal::Api::Common::V1::Payload)) : Bool
        handler = @signal_handlers[name]?
        if handler
          handler.call(payloads)
          true
        else
          false
        end
      end

      # Handle a query — dispatch to registered handler. Returns result payload or nil.
      def handle_query(
        query_id : String,
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload)
      ) : Temporal::Api::Common::V1::Payload?
        handler = @query_handlers[name]?
        handler.try(&.call(payloads))
      end

      # Queue an update handler for execution.
      # Called by WorkflowInstance after validation passes (or if no validator).
      def queue_update_handler(
        protocol_instance_id : String,
        name : String,
        input : Array(Temporal::Api::Common::V1::Payload)
      ) : Nil
        @pending_updates[protocol_instance_id] = PendingUpdate.new(
          protocol_instance_id: protocol_instance_id,
          name: name,
          input: input
        )
      end

      # Execute queued update handlers.
      # Called when workflow fiber resumes after DoUpdate jobs are applied.
      # This method is called from WorkflowInstance after fiber processes other jobs.
      def execute_queued_update_handlers(
        workflow_object : Internal::WorkflowObject,
        data_converter : DataConverter
      ) : Nil
        @pending_updates.each do |protocol_instance_id, update|
          begin
            # Thread through handle_update interceptors
            update_input = Interceptor::HandleUpdateInput.new(
              update_id: protocol_instance_id,
              update_name: update.name,
              args: update.input
            )
            chain = @interceptors.reverse
            inner_fn = Proc(Interceptor::HandleUpdateInput, Temporal::Api::Common::V1::Payload?).new do |inp|
              workflow_object._temporal_handle_update(
                inp.update_name,
                inp.args,
                data_converter
              )
            end
            update_fn = chain.reduce(inner_fn) do |next_fn, interceptor|
              Proc(Interceptor::HandleUpdateInput, Temporal::Api::Common::V1::Payload?).new do |i|
                interceptor.handle_update(i, next_fn)
              end
            end
            # Execute handler (can yield, use workflow operations)
            result = update_fn.call(update_input)

            # Send Completed response
            enqueue_command(
              Coresdk::WorkflowCommands::WorkflowCommand.new(
                update_response: Coresdk::WorkflowCommands::UpdateResponse.new(
                  protocol_instance_id: protocol_instance_id,
                  completed: result
                )
              )
            )
          rescue ex
            # Send Rejected response
            failure = Internal::FailureConverter.to_failure(ex, data_converter)
            enqueue_command(
              Coresdk::WorkflowCommands::WorkflowCommand.new(
                update_response: Coresdk::WorkflowCommands::UpdateResponse.new(
                  protocol_instance_id: protocol_instance_id,
                  rejected: failure
                )
              )
            )
          end

          @pending_updates.delete(protocol_instance_id)
        end
      end

      # Mark the workflow as cancelled (called when CancelWorkflow job arrives).
      def request_cancel! : Nil
        @cancelled = true
      end

      # True after a CancelWorkflow job has been applied.
      def cancelled? : Bool
        @cancelled
      end

      # Record that a patch has been activated server-side.
      def record_patch(patch_id : String) : Nil
        @recorded_patches.add(patch_id)
      end

      # Returns true if this workflow has seen the given patch.
      # Also schedules a SetPatchMarker command so future replays know about it.
      def patched?(patch_id : String) : Bool
        unless @recorded_patches.includes?(patch_id)
          @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
            set_patch_marker: Coresdk::WorkflowCommands::SetPatchMarker.new(
              patch_id: patch_id,
              deprecated: false
            )
          )
        end
        @recorded_patches.includes?(patch_id)
      end

      # Execute an activity by class reference — typed args and return value.
      # Encodes args, calls _execute_activity, decodes the result.
      #
      # Usage:
      #   workflow = Temporalio::Workflow::Context.current
      #   result = workflow.execute_activity(MyActivity, "arg1", start_to_close_timeout: 10.seconds)
      def execute_activity(activity_klass : T.class, *args, **options) forall T
        {% begin %}
          {% execute_method = T.methods.find { |m| m.name == "execute" } %}
          {% if execute_method %}
            {% return_type = execute_method.return_type %}
          {% else %}
            {% return_type = Nil %}
          {% end %}
          payloads = args.to_a.map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
          result_payload = _execute_activity(T.activity_name, payloads, **options)
          {% if return_type.stringify == "Nil" %}
            nil
          {% else %}
            @data_converter.from_payload(result_payload.not_nil!, {{return_type}})
          {% end %}
        {% end %}
      end

      # Execute a local activity by class reference — typed args and return value.
      # Encodes args, calls _execute_local_activity, decodes the result.
      #
      # Usage:
      #   workflow = Temporalio::Workflow::Context.current
      #   result = workflow.execute_local_activity(MyActivity, "arg1", start_to_close_timeout: 10.seconds)
      def execute_local_activity(activity_klass : T.class, *args, **options) forall T
        {% begin %}
          {% execute_method = T.methods.find { |m| m.name == "execute" } %}
          {% if execute_method %}
            {% return_type = execute_method.return_type %}
          {% else %}
            {% return_type = Nil %}
          {% end %}
          payloads = args.to_a.map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
          result_payload = _execute_local_activity(T.activity_name, payloads, **options)
          {% if return_type.stringify == "Nil" %}
            nil
          {% else %}
            @data_converter.from_payload(result_payload.not_nil!, {{return_type}})
          {% end %}
        {% end %}
      end

      # Execute a child workflow by class reference — typed args and return value.
      # Encodes args, calls _execute_child_workflow, decodes the result.
      #
      # Usage:
      #   workflow = Temporalio::Workflow::Context.current
      #   result = workflow.execute_child_workflow(ChildWorkflow, "arg1", workflow_id: "my-id")
      def execute_child_workflow(workflow_klass : T.class, *args, **options) forall T
        {% begin %}
          {% execute_method = T.methods.find { |m| m.name == "execute" } %}
          {% if execute_method %}
            {% return_type = execute_method.return_type %}
          {% else %}
            {% return_type = Nil %}
          {% end %}
          payloads = args.to_a.map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
          result_payload = _execute_child_workflow(T.workflow_name, payloads, **options)
          {% if return_type.stringify == "Nil" %}
            nil
          {% else %}
            @data_converter.from_payload(result_payload.not_nil!, {{return_type}})
          {% end %}
        {% end %}
      end

      # Schedule an activity and yield until it resolves.
      # Returns the result payload on success, raises on failure/cancellation.
      # Use the Temporalio::Workflow.execute_activity macro instead.
      def _execute_activity(
        activity_type : String,
        args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
        task_queue : String? = nil,
        schedule_to_close_timeout : Time::Span? = nil,
        schedule_to_start_timeout : Time::Span? = nil,
        start_to_close_timeout : Time::Span? = nil,
        heartbeat_timeout : Time::Span? = nil,
        retry_policy : Client::RetryPolicy? = nil,
        cancellation_type : ActivityCancellationType = ActivityCancellationType::TRY_CANCEL
      ) : Temporal::Api::Common::V1::Payload?
        seq = alloc_seq
        resolution_ch = Channel(Coresdk::ActivityResult::ActivityResolution).new(1)
        @pending_activities[seq] = resolution_ch

        target_task_queue = task_queue || @task_queue
        
        # Convert retry policy if provided
        proto_retry_policy = if retry_policy
          Temporal::Api::Common::V1::RetryPolicy.new(
            initial_interval: span_to_duration(retry_policy.initial_interval),
            backoff_coefficient: retry_policy.backoff_coefficient,
            maximum_interval: span_to_duration(retry_policy.maximum_interval),
            maximum_attempts: retry_policy.maximum_attempts
          )
        end
        
        @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
          schedule_activity: Coresdk::WorkflowCommands::ScheduleActivity.new(
            seq: seq,
            activity_id: seq.to_s,
            activity_type: activity_type,
            task_queue: target_task_queue,
            arguments: args,
            schedule_to_close_timeout: span_to_duration(schedule_to_close_timeout),
            schedule_to_start_timeout: span_to_duration(schedule_to_start_timeout),
            start_to_close_timeout: span_to_duration(start_to_close_timeout),
            heartbeat_timeout: span_to_duration(heartbeat_timeout),
            retry_policy: proto_retry_policy,
            cancellation_type: cancellation_type.value
          )
        )

        yield_to_poller
        
        # Keep yielding until the resolution is available
        resolution = nil
        while resolution.nil?
          # Use select with immediate timeout to check if data is available
          select
          when val = resolution_ch.receive
            resolution = val
          else
            yield_to_poller
          end
        end

        if failure_info = resolution.failed
          if f = failure_info.failure
            raise Internal::FailureConverter.from_failure(f, @data_converter)
          end
        end

        if resolution.cancelled
          raise CancelledError.new("Activity cancelled")
        end

        resolution.completed.try(&.result)
      end

      # Schedule a local activity and yield until it resolves.
      # Returns the result payload on success, raises on failure/cancellation.
      # Use the Temporalio::Workflow.execute_local_activity macro instead.
      def _execute_local_activity(
        activity_type : String,
        args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
        schedule_to_close_timeout : Time::Span? = nil,
        schedule_to_start_timeout : Time::Span? = nil,
        start_to_close_timeout : Time::Span? = nil,
        retry_policy : Client::RetryPolicy? = nil,
        cancellation_type : ActivityCancellationType = ActivityCancellationType::TRY_CANCEL
      ) : Temporal::Api::Common::V1::Payload?
        seq = alloc_seq
        resolution_ch = Channel(Coresdk::ActivityResult::ActivityResolution).new(1)
        @pending_activities[seq] = resolution_ch

        @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
          schedule_local_activity: Coresdk::WorkflowCommands::ScheduleLocalActivity.new(
            seq: seq,
            activity_id: seq.to_s,
            activity_type: activity_type,
            arguments: args,
            schedule_to_close_timeout: span_to_duration(schedule_to_close_timeout),
            schedule_to_start_timeout: span_to_duration(schedule_to_start_timeout),
            start_to_close_timeout: span_to_duration(start_to_close_timeout),
            cancellation_type: cancellation_type.value
          )
        )

        yield_to_poller
        resolution = resolution_ch.receive

        if failure_info = resolution.failed
          if f = failure_info.failure
            raise Internal::FailureConverter.from_failure(f, @data_converter)
          end
        end

        resolution.completed.try(&.result)
      end

      # Start a child workflow and wait for it to complete.
      # Returns the result payload on success, raises on failure/cancellation.
      # Use the Temporalio::Workflow.execute_child_workflow macro instead.
      def _execute_child_workflow(
        workflow_type : String,
        args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
        workflow_id : String? = nil,
        task_queue : String? = nil,
        execution_timeout : Time::Span? = nil,
        run_timeout : Time::Span? = nil,
        task_timeout : Time::Span? = nil,
        id_reuse_policy : Int32 = 0,
        retry_policy : Client::RetryPolicy? = nil,
        cron_schedule : String? = nil,
        cancellation_type : ChildWorkflowCancellationType = ChildWorkflowCancellationType::WAIT_CANCELLATION_COMPLETED,
        parent_close_policy : ParentClosePolicy = ParentClosePolicy::TERMINATE,
        memo : Hash(String, String)? = nil,
        search_attributes : Hash(String, String)? = nil
      ) : Temporal::Api::Common::V1::Payload?
        # Notify interceptors (observation only — execution always proceeds)
        intercept_input = Interceptor::ExecuteChildWorkflowInput.new(
          workflow_type: workflow_type,
          args: args,
          workflow_id: workflow_id,
          task_queue: task_queue
        )
        chain = @interceptors.reverse
        inner_notify = Proc(Interceptor::ExecuteChildWorkflowInput, Nil).new { |_| nil }
        notify_fn = chain.reduce(inner_notify) do |next_fn, interceptor|
          Proc(Interceptor::ExecuteChildWorkflowInput, Nil).new do |i|
            interceptor.execute_child_workflow(i, next_fn)
          end
        end
        notify_fn.call(intercept_input)
        seq = alloc_seq
        child_wf_id = workflow_id || "#{@workflow_id}-child-#{seq}"

        start_ch = Channel(Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStart).new(1)
        complete_ch = Channel(Coresdk::ChildWorkflow::ChildWorkflowResult).new(1)
        @pending_child_start[seq] = start_ch
        @pending_child_complete[seq] = complete_ch

        proto_retry_policy_child = if retry_policy
          Temporal::Api::Common::V1::RetryPolicy.new(
            initial_interval: span_to_duration(retry_policy.initial_interval),
            backoff_coefficient: retry_policy.backoff_coefficient,
            maximum_interval: span_to_duration(retry_policy.maximum_interval),
            maximum_attempts: retry_policy.maximum_attempts
          )
        end

        proto_memo = if memo
          entries = memo.map do |k, v|
            Coresdk::WorkflowCommands::StringPayloadEntry.new(key: k, value: @data_converter.to_payload(v))
          end
          entries
        end

        proto_search_attrs = if search_attributes
          entries = search_attributes.map do |k, v|
            Coresdk::WorkflowCommands::StringPayloadEntry.new(key: k, value: @data_converter.to_payload(v))
          end
          entries
        end

        @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
          start_child_workflow_execution: Coresdk::WorkflowCommands::StartChildWorkflowExecution.new(
            seq: seq,
            workflow_id: child_wf_id,
            workflow_type: workflow_type,
            task_queue: task_queue || @task_queue,
            input: args,
            workflow_execution_timeout: span_to_duration(execution_timeout),
            workflow_run_timeout: span_to_duration(run_timeout),
            workflow_task_timeout: span_to_duration(task_timeout),
            workflow_id_reuse_policy: id_reuse_policy,
            retry_policy: proto_retry_policy_child,
            cron_schedule: cron_schedule,
            cancellation_type: cancellation_type.value,
            parent_close_policy: parent_close_policy.value,
            memo: proto_memo,
            search_attributes: proto_search_attrs
          )
        )

        # Wait for child to start
        yield_to_poller
        
        start_result = nil
        until start_result
          check_cancellation!
          select
          when result = start_ch.receive
            start_result = result
          else
            yield_to_poller
          end
        end

        if cancelled = start_result.cancelled
          raise CancelledError.new("Child workflow start cancelled")
        end

        if start_failed = start_result.failed
          raise Error.new("Child workflow #{child_wf_id} failed to start: #{start_failed.cause}")
        end

        # Wait for child to complete
        yield_to_poller
        
        complete_result = nil
        until complete_result
          check_cancellation!
          select
          when result = complete_ch.receive
            complete_result = result
          else
            yield_to_poller
          end
        end

        if completed = complete_result.completed
          return completed.result
        end

        if failed = complete_result.failed
          if f = failed.failure
            raise Internal::FailureConverter.from_failure(f, @data_converter)
          end
        end

        if complete_result.cancelled
          raise CancelledError.new("Child workflow #{child_wf_id} was cancelled")
        end

        nil
      end

      # Send a signal to an external workflow by workflow ID.
      # Raises on failure (e.g. workflow not found).
      def _signal_external_workflow(
        workflow_id : String,
        signal_name : String,
        args : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload,
        run_id : String? = nil,
        namespace : String? = nil
      ) : Nil
        intercept_input = Interceptor::SignalExternalWorkflowInput.new(
          workflow_id: workflow_id,
          signal_name: signal_name,
          args: args,
          run_id: run_id,
          namespace: namespace
        )
        chain = @interceptors.reverse
        inner = Proc(Interceptor::SignalExternalWorkflowInput, Nil).new do |inp|
          do_signal_external_workflow(inp.workflow_id, inp.signal_name, inp.args, inp.run_id, inp.namespace)
        end
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Interceptor::SignalExternalWorkflowInput, Nil).new do |i|
            interceptor.signal_external_workflow(i, next_fn)
          end
        end
        fn.call(intercept_input)
      end

      private def do_signal_external_workflow(
        workflow_id : String,
        signal_name : String,
        args : Array(Temporal::Api::Common::V1::Payload),
        run_id : String?,
        namespace : String?
      ) : Nil
        seq = alloc_seq
        result_ch = Channel(Coresdk::WorkflowActivation::ResolveSignalExternalWorkflow).new(1)
        @pending_external_signals[seq] = result_ch

        wf_exec = Coresdk::Common::NamespacedWorkflowExecution.new(
          namespace: namespace || @namespace,
          workflow_id: workflow_id,
          run_id: run_id || ""
        )

        @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
          signal_external_workflow_execution: Coresdk::WorkflowCommands::SignalExternalWorkflowExecution.new(
            seq: seq,
            workflow_execution: wf_exec,
            signal_name: signal_name,
            args: args
          )
        )

        yield_to_poller
        result = result_ch.receive

        if f = result.failure
          raise Internal::FailureConverter.from_failure(f, @data_converter)
        end
      end

      # Request cancellation of an external workflow.
      # Raises on failure (e.g. workflow not found).
      def _cancel_external_workflow(
        workflow_id : String,
        run_id : String? = nil,
        namespace : String? = nil,
        reason : String? = nil
      ) : Nil
        intercept_input = Interceptor::CancelExternalWorkflowInput.new(
          workflow_id: workflow_id,
          run_id: run_id,
          namespace: namespace,
          reason: reason
        )
        chain = @interceptors.reverse
        inner = Proc(Interceptor::CancelExternalWorkflowInput, Nil).new do |inp|
          do_cancel_external_workflow(inp.workflow_id, inp.run_id, inp.namespace, inp.reason)
        end
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Interceptor::CancelExternalWorkflowInput, Nil).new do |i|
            interceptor.cancel_external_workflow(i, next_fn)
          end
        end
        fn.call(intercept_input)
      end

      private def do_cancel_external_workflow(
        workflow_id : String,
        run_id : String?,
        namespace : String?,
        reason : String?
      ) : Nil
        seq = alloc_seq
        result_ch = Channel(Coresdk::WorkflowActivation::ResolveRequestCancelExternalWorkflow).new(1)
        @pending_external_cancels[seq] = result_ch

        wf_exec = Coresdk::Common::NamespacedWorkflowExecution.new(
          namespace: namespace || @namespace,
          workflow_id: workflow_id,
          run_id: run_id || ""
        )

        @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
          request_cancel_external_workflow_execution: Coresdk::WorkflowCommands::RequestCancelExternalWorkflowExecution.new(
            seq: seq,
            workflow_execution: wf_exec,
            reason: reason || ""
          )
        )

        yield_to_poller
        result = result_ch.receive

        if f = result.failure
          raise Internal::FailureConverter.from_failure(f, @data_converter)
        end
      end

      # Schedule a timer and yield until it fires.
      def sleep(duration : Time::Span) : Nil
        input = Interceptor::StartTimerInput.new(duration: duration)
        chain = @interceptors.reverse
        inner = Proc(Interceptor::StartTimerInput, Nil).new do |inp|
          do_sleep(inp.duration)
        end
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Interceptor::StartTimerInput, Nil).new do |i|
            interceptor.start_timer(i, next_fn)
          end
        end
        fn.call(input)
      end

      private def do_sleep(duration : Time::Span) : Nil
        seq = alloc_seq
        timer_ch = Channel(Nil).new(1)
        @pending_timers[seq] = timer_ch
        timer_fired = false

        @commands << Coresdk::WorkflowCommands::WorkflowCommand.new(
          start_timer: Coresdk::WorkflowCommands::StartTimer.new(
            seq: seq,
            start_to_fire_timeout: span_to_duration(duration)
          )
        )

        yield_to_poller

        # Wait for timer to fire, checking cancellation on each activation
        until timer_fired
          check_cancellation!

          # Check if timer has fired (non-blocking)
          select
          when timer_ch.receive
            timer_fired = true
          else
            # Timer not ready yet, yield to next activation
            yield_to_poller
          end
        end
      end

      # Wait until condition returns true. Yields at each activation.
      def wait_condition(&condition : -> Bool) : Nil
        until condition.call
          yield_to_poller
        end
      end

      # Raise CancelledError if the workflow has been cancelled.
      def check_cancellation! : Nil
        if @cancelled
          raise CancelledError.new("Workflow cancelled")
        end
      end

      # Continue-as-new with the same or different workflow type.
      # Encodes each arg with the data converter.
      # Raises ContinueAsNewError which the worker catches and converts to
      # a ContinueAsNewWorkflowExecution command.
      def continue_as_new(
        *raw_args,
        workflow_type : String? = nil,
        task_queue : String? = nil,
        execution_timeout : Time::Span? = nil,
        run_timeout : Time::Span? = nil,
        task_timeout : Time::Span? = nil
      ) : Nil
        encoded_args = raw_args.to_a.map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
        intercept_input = Interceptor::ContinueAsNewInput.new(
          workflow_type: workflow_type,
          args: encoded_args,
          task_queue: task_queue || @task_queue
        )
        chain = @interceptors.reverse
        inner = Proc(Interceptor::ContinueAsNewInput, Nil).new do |inp|
          raise ContinueAsNewError.new(
            args: inp.args,
            workflow_type: inp.workflow_type,
            task_queue: inp.task_queue || @task_queue,
            execution_timeout: execution_timeout,
            run_timeout: run_timeout,
            task_timeout: task_timeout
          )
        end
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Interceptor::ContinueAsNewInput, Nil).new do |i|
            interceptor.continue_as_new(i, next_fn)
          end
        end
        fn.call(intercept_input)
      end

      # Resolve a timer fire job.
      def resolve_timer(seq : UInt32) : Nil
        @pending_timers.delete(seq).try(&.send(nil))
      end

      # Resolve an activity result job.
      def resolve_activity(seq : UInt32, resolution : Coresdk::ActivityResult::ActivityResolution) : Nil
        @pending_activities.delete(seq).try(&.send(resolution))
      end

      # Resolve a child workflow start job.
      def resolve_child_workflow_start(
        seq : UInt32,
        result : Coresdk::WorkflowActivation::ResolveChildWorkflowExecutionStart
      ) : Nil
        @pending_child_start.delete(seq).try(&.send(result))
      end

      # Resolve a child workflow completion job.
      def resolve_child_workflow(seq : UInt32, result : Coresdk::ChildWorkflow::ChildWorkflowResult) : Nil
        @pending_child_complete.delete(seq).try(&.send(result))
      end

      # Resolve an external signal delivery.
      def resolve_external_signal(seq : UInt32, result : Coresdk::WorkflowActivation::ResolveSignalExternalWorkflow) : Nil
        @pending_external_signals.delete(seq).try(&.send(result))
      end

      # Resolve an external cancellation request.
      def resolve_external_cancel(seq : UInt32, result : Coresdk::WorkflowActivation::ResolveRequestCancelExternalWorkflow) : Nil
        @pending_external_cancels.delete(seq).try(&.send(result))
      end

      # Returns a handle to an external workflow by ID for signalling or cancelling.
      def get_external_workflow_handle(workflow_id : String, run_id : String? = nil) : ExternalWorkflowHandle
        ExternalWorkflowHandle.new(self, workflow_id, run_id)
      end

      # Yield control back to the poller fiber.
      def yield_to_poller : Nil
        @suspended_channel.send(nil)
        @resume_channel.receive
      end

      # Resume the workflow fiber from the poller side.
      def resume! : Nil
        @resume_channel.send(nil)
        @suspended_channel.receive
      end

      # Install this context for the current fiber.
      def install! : Nil
        Fiber.current.temporalio_workflow_context = self
      end

      # Uninstall this context when the workflow fiber exits.
      def uninstall! : Nil
        Fiber.current.temporalio_workflow_context = nil
      end

      private def alloc_seq : UInt32
        seq = @next_seq
        @next_seq += 1
        seq
      end

      private def span_to_duration(span : Time::Span?) : Google::Protobuf::Duration?
        return nil if span.nil?
        Google::Protobuf::Duration.new(
          seconds: span.total_seconds.to_i64,
          nanos: span.nanoseconds
        )
      end

    end

    # Handle to an external workflow for signalling or requesting cancellation.
    # Obtained via workflow.get_external_workflow_handle(id).
    class ExternalWorkflowHandle
      def initialize(
        @context : Context,
        @workflow_id : String,
        @run_id : String?
      )
      end

      # Send a signal to the external workflow.
      def signal(signal_name : String, *args) : Nil
        payloads = args.to_a.map { |a| @context.data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
        @context._signal_external_workflow(@workflow_id, signal_name, payloads, run_id: @run_id)
      end

      # Request cancellation of the external workflow.
      def cancel(reason : String? = nil) : Nil
        @context._cancel_external_workflow(@workflow_id, run_id: @run_id, reason: reason)
      end
    end
  end

end
