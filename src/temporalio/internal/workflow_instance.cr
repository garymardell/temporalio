require "../internal/proto"
require "../internal/failure_converter"
require "../data_converter"
require "../workflow"
require "../interceptor/worker_interceptor"

module Temporalio
  module Internal
    # Abstract type-erased workflow object.
    abstract class WorkflowObject
      abstract def _temporal_execute(
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?

      abstract def _temporal_handle_signal(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Nil

      abstract def _temporal_handle_query(
        query_id : String,
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?

      abstract def _temporal_handle_update(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?

      abstract def _temporal_handle_update_validator(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Nil
    end

    # Concrete wrapper for T which includes Temporalio::Workflow.
    class ConcreteWorkflowObject(T) < WorkflowObject
      def initialize
        @instance = T.new
      end

      def _temporal_execute(
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?
        @instance._temporal_execute(payloads, converter)
      end

      def _temporal_handle_signal(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Nil
        @instance._temporal_handle_signal(name, payloads, converter)
      end

      def _temporal_handle_query(
        query_id : String,
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?
        @instance._temporal_handle_query(query_id, name, payloads, converter)
      end

      def _temporal_handle_update(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Temporal::Api::Common::V1::Payload?
        @instance._temporal_handle_update(name, payloads, converter)
      end

      def _temporal_handle_update_validator(
        name : String,
        payloads : Array(Temporal::Api::Common::V1::Payload),
        converter : DataConverter
      ) : Nil
        @instance._temporal_handle_update_validator(name, payloads, converter)
      end
    end

    # Represents one live workflow run.
    #
    # Fiber coordination protocol:
    #
    #   Poller fiber                         Workflow fiber
    #   ──────────────────────────────────   ─────────────────────────────
    #   apply jobs
    #
    #   First activation:
    #     spawn workflow fiber ────────────→  runs execute(...)
    #     wait on @ready_ch                   if needs to wait: yield_to_poller
    #                                           send on @ready_ch (poller wakes)
    #                                           wait on @resume_ch
    #     @ready_ch fires
    #     collect commands
    #     ...next activation...
    #     fill resolution channels
    #     send on @resume_ch ──────────────→  resume_ch fires, continue execute
    #     wait on @ready_ch ←──────────────   yield_to_poller: send on @ready_ch
    #     collect commands
    #
    #   When workflow finishes (execute returns or raises):
    #     result_ch.send(...)
    #     ready_ch.send(nil)   ← wakes the poller's wait
    #
    # @ready_ch replaces the old @suspended_channel — it fires either when:
    #   (a) the workflow fiber calls yield_to_poller, OR
    #   (b) the workflow fiber finishes
    class WorkflowInstance
      getter run_id : String
      getter workflow_type : String
      getter? complete : Bool = false

      @workflow_fiber : Fiber
      @workflow_fiber_started : Bool = false
      @workflow_object : WorkflowObject
      @interceptors : Array(Temporalio::Interceptor::WorkerInterceptor)

      def initialize(
        activation : Coresdk::WorkflowActivation::WorkflowActivation,
        @workflow_object : WorkflowObject,
        @data_converter : DataConverter,
        namespace : String,
        task_queue : String,
        @interceptors : Array(Temporalio::Interceptor::WorkerInterceptor) = [] of Temporalio::Interceptor::WorkerInterceptor
      )
        @run_id = activation.run_id || ""

        # @ready_ch: workflow fiber → poller. Fires when fiber yields or finishes.
        # @resume_ch: poller → workflow fiber. Fires to wake the fiber.
        # Both buffered(1) so send never blocks if receiver hasn't arrived yet.
        @ready_ch = Channel(Nil).new(1)
        @resume_ch = Channel(Nil).new(1)

        # Sends {payload?, exception?} when workflow fiber finishes.
        @result_channel = Channel(Tuple(Temporal::Api::Common::V1::Payload?, Exception?)).new(1)

        jobs = activation.jobs || [] of Coresdk::WorkflowActivation::WorkflowActivationJob
        init_job = jobs.find(&.initialize_workflow)
        init = init_job.try(&.initialize_workflow)

        @workflow_type = init.try(&.workflow_type) || ""
        real_workflow_id = init.try(&.workflow_id) || ""
        attempt = init.try(&.attempt) || 1
        now = timestamp_to_time(activation.timestamp)
        start_time = timestamp_to_time(init.try(&.start_time))
        random_seed = init.try(&.randomness_seed) || 0_u64
        continued_run_id = init.try(&.continued_from_execution_run_id)
        continued_run_id = nil if continued_run_id && continued_run_id.empty?
        first_execution_run_id = init.try(&.first_execution_run_id)
        first_execution_run_id = nil if first_execution_run_id && first_execution_run_id.empty?
        cron_schedule = init.try(&.cron_schedule)
        cron_schedule = nil if cron_schedule && cron_schedule.empty?

        parent_info : Workflow::Context::ParentInfo? = nil
        if p = init.try(&.parent_workflow_info)
          parent_info = Workflow::Context::ParentInfo.new(
            p.namespace || "",
            p.workflow_id || "",
            p.run_id || ""
          )
        end

        root_info : Workflow::Context::RootInfo? = nil
        if r = init.try(&.root_workflow)
          rw_id = r.workflow_id || ""
          rr_id = r.run_id || ""
          root_info = Workflow::Context::RootInfo.new(rw_id, rr_id) unless rw_id.empty? && rr_id.empty?
        end

        retry_policy : Client::RetryPolicy? = nil
        if rp = init.try(&.retry_policy)
          retry_policy = Client::RetryPolicy.new(
            initial_interval: proto_duration_to_span(rp.initial_interval) || 1.second,
            backoff_coefficient: rp.backoff_coefficient || 2.0,
            maximum_interval: proto_duration_to_span(rp.maximum_interval),
            maximum_attempts: rp.maximum_attempts || 0,
            non_retryable_error_types: rp.non_retryable_error_types || [] of String
          )
        end

        memo_hash = {} of String => Array(Temporal::Api::Common::V1::Payload)
        if m = init.try(&.memo)
          (m.fields || [] of Temporal::Api::Common::V1::MemoFieldsEntry).each do |entry|
            k = entry.key || ""
            next if k.empty?
            v = entry.value
            memo_hash[k] = [v].compact if v
          end
        end

        sa_hash = {} of String => Array(Temporal::Api::Common::V1::Payload)
        if sa = init.try(&.search_attributes)
          (sa.indexed_fields || [] of Temporal::Api::Common::V1::StringPayloadEntry).each do |entry|
            k = entry.key || ""
            next if k.empty?
            v = entry.value
            sa_hash[k] = [v].compact if v
          end
        end

        @context = Workflow::Context.new(
          run_id: @run_id,
          workflow_type: @workflow_type,
          workflow_id: real_workflow_id,
          namespace: namespace,
          task_queue: task_queue,
          attempt: attempt,
          data_converter: @data_converter,
          now: now,
          start_time: start_time,
          parent: parent_info,
          continued_run_id: continued_run_id,
          history_length: activation.history_length || 0_u32,
          history_size_bytes: activation.history_size_bytes || 0_u64,
          continue_as_new_suggested: activation.continue_as_new_suggested || false,
          replaying: activation.is_replaying || false,
          random_seed: random_seed,
          resume_channel: @resume_ch,
          suspended_channel: @ready_ch,
          execution_timeout: proto_duration_to_span(init.try(&.workflow_execution_timeout)),
          run_timeout: proto_duration_to_span(init.try(&.workflow_run_timeout)),
          task_timeout: proto_duration_to_span(init.try(&.workflow_task_timeout)),
          retry_policy: retry_policy,
          cron_schedule: cron_schedule,
          memo: memo_hash,
          search_attributes: sa_hash,
          root_workflow: root_info,
          first_execution_run_id: first_execution_run_id,
          interceptors: @interceptors
        )
        # Give the context a reference to the workflow object so it can spawn
        # update handler fibers from within the workflow fiber.
        @context.workflow_object = @workflow_object

        input_payloads = init.try(&.arguments) || [] of Temporal::Api::Common::V1::Payload

        wf_obj = @workflow_object
        dc = @data_converter
        ctx = @context
        res_ch = @result_channel
        ready_ch = @ready_ch

        # Use spawn (scheduler-managed) instead of Fiber.new + resume,
        # so the fiber runs cooperatively with the event loop.
        @workflow_fiber = Fiber.new do
          ctx.install!
          begin
            payload = wf_obj._temporal_execute(input_payloads, dc)
            res_ch.send({payload, nil})
          rescue ex
            res_ch.send({nil, ex})
          ensure
            ctx.uninstall!
          end
          # Always signal the poller that the fiber is done.
          ready_ch.send(nil)
        end
      end

      # Apply a workflow activation and return the completion to send to Core.
      def apply_activation(
        activation : Coresdk::WorkflowActivation::WorkflowActivation
      ) : Coresdk::WorkflowCompletion::WorkflowActivationCompletion
        run_id = activation.run_id || ""

        @context.update_time(activation.timestamp)
        @context.update_activation_info(
          history_length: activation.history_length || 0_u32,
          history_size_bytes: activation.history_size_bytes || 0_u64,
          continue_as_new_suggested: activation.continue_as_new_suggested || false,
          replaying: activation.is_replaying || false
        )

        jobs = activation.jobs || [] of Coresdk::WorkflowActivation::WorkflowActivationJob

        if jobs.any?(&.remove_from_cache)
          rfc_job = jobs.find(&.remove_from_cache)
          if rfc_job && (rfc = rfc_job.remove_from_cache)
          end
          @complete = true
          return activation_completion(run_id, [] of Coresdk::WorkflowCommands::WorkflowCommand)
        end

        # Separate query jobs from fiber-waking jobs.
        # Queries are read-only and answered in the poller fiber without resuming
        # the workflow fiber. All other jobs (timers, activities, signals, etc.)
        # require the fiber to be scheduled.
        query_jobs, fiber_jobs = jobs.partition(&.query_workflow)

        # Apply non-query jobs (signals, timer fires, activity resolves, etc.)
        fiber_jobs.each { |job| apply_job(job) }

        # Always apply query jobs (enqueues respond_to_query commands).
        query_jobs.each { |job| apply_job(job) }

        # If there are no fiber-waking jobs and the fiber is already running,
        # return the query responses immediately without touching the workflow fiber.
        if fiber_jobs.empty? && @workflow_fiber_started
          return activation_completion(run_id, @context.drain_commands)
        end

        if !@workflow_fiber_started
          @workflow_fiber_started = true
          # Start the workflow fiber using spawn (which properly schedules it)
          @workflow_fiber.enqueue
          # Wait for it to yield or complete
          @ready_ch.receive
        else
          # Wake the sleeping workflow fiber by sending on @resume_ch.
          # yield_to_poller is: send on @ready_ch, receive on @resume_ch.
          # So: we send on @resume_ch, then wait for @ready_ch.
          @resume_ch.send(nil)
          @ready_ch.receive
        end

        # Non-blocking check: did the fiber finish?
        result_tuple = select
          when rt = @result_channel.receive?
            rt
          else
            nil
          end

        if rt = result_tuple
          result_payload, error = rt
          @complete = true

          cmds = @context.drain_commands
          if err = error
            if can_err = err.as?(Temporalio::ContinueAsNewError)
              # Emit ContinueAsNewWorkflowExecution command instead of failure.
              cmds << Coresdk::WorkflowCommands::WorkflowCommand.new(
                continue_as_new_workflow_execution: Coresdk::WorkflowCommands::ContinueAsNewWorkflowExecution.new(
                  workflow_type: can_err.workflow_type,
                  task_queue: can_err.task_queue,
                  arguments: can_err.args,
                  workflow_run_timeout: span_to_duration(can_err.run_timeout),
                  workflow_task_timeout: span_to_duration(can_err.task_timeout)
                )
              )
            else
              failure = FailureConverter.to_failure(err, @data_converter)
              cmds << Coresdk::WorkflowCommands::WorkflowCommand.new(
                fail_workflow_execution: Coresdk::WorkflowCommands::FailWorkflowExecution.new(
                  failure: failure
                )
              )
            end
          else
            cmds << Coresdk::WorkflowCommands::WorkflowCommand.new(
              complete_workflow_execution: Coresdk::WorkflowCommands::CompleteWorkflowExecution.new(
                result: result_payload
              )
            )
          end
          return activation_completion(run_id, cmds)
        end

        activation_completion(run_id, @context.drain_commands)
      end

      private def apply_job(job : Coresdk::WorkflowActivation::WorkflowActivationJob) : Nil
        return if job.initialize_workflow

        if fire = job.fire_timer
          @context.resolve_timer(fire.seq || 0_u32)

        elsif resolve = job.resolve_activity
          if res = resolve.result
            @context.resolve_activity(resolve.seq || 0_u32, res)
          end

        elsif resolve = job.resolve_child_workflow_execution_start
          @context.resolve_child_workflow_start(resolve.seq || 0_u32, resolve)

        elsif resolve = job.resolve_child_workflow_execution
          if res = resolve.result
            @context.resolve_child_workflow(resolve.seq || 0_u32, res)
          end

        elsif signal = job.signal_workflow
          name = signal.signal_name || ""
          payloads = signal.input || [] of Temporal::Api::Common::V1::Payload
          sig_input = Temporalio::Interceptor::HandleSignalInput.new(
            signal_name: name,
            args: payloads
          )
          run_signal_interceptors(sig_input) do |inp|
            handled = @context.handle_signal(inp.signal_name, inp.args)
            unless handled
              @workflow_object._temporal_handle_signal(inp.signal_name, inp.args, @data_converter)
            end
          end

        elsif query = job.query_workflow
          query_id = query.query_id || ""
          query_type = query.query_type || ""
          args = query.arguments || [] of Temporal::Api::Common::V1::Payload
          query_input = Temporalio::Interceptor::HandleQueryInput.new(
            query_id: query_id,
            query_type: query_type,
            args: args
          )
          result = run_query_interceptors(query_input) do |inp|
            r = @context.handle_query(inp.query_id, inp.query_type, inp.args)
            r ||= @workflow_object._temporal_handle_query(inp.query_id, inp.query_type, inp.args, @data_converter)
            r
          end
          @context.enqueue_command(Coresdk::WorkflowCommands::WorkflowCommand.new(
            respond_to_query: Coresdk::WorkflowCommands::QueryResult.new(
              query_id: query_id,
              succeeded: Coresdk::WorkflowCommands::QuerySuccess.new(response: result)
            )
          ))

        elsif update = job.do_update
          handle_update_job(update)

        elsif job.cancel_workflow
          @context.request_cancel!

        elsif patch = job.notify_has_patch
          @context.record_patch(patch.patch_id || "")

        elsif seed_update = job.update_random_seed
          @context.update_random_seed(seed_update.randomness_seed || 0_u64)

        elsif resolve = job.resolve_signal_external_workflow
          @context.resolve_external_signal(resolve.seq || 0_u32, resolve)

        elsif resolve = job.resolve_request_cancel_external_workflow
          @context.resolve_external_cancel(resolve.seq || 0_u32, resolve)
        end
      end

      private def handle_update_job(update : Coresdk::WorkflowActivation::DoUpdate) : Nil
        protocol_instance_id = update.protocol_instance_id || ""
        update_name = update.name || ""
        input = update.input || [] of Temporal::Api::Common::V1::Payload

        # Phase 1: Validation (synchronous, in poller fiber).
        # run_validator controls whether to run the validator code, but does NOT control
        # whether to send the `accepted` command. Per the Core SDK proto:
        #   "Must be sent if the update's validator has passed (or lang was not asked to
        #    run it, and thus should be considered already-accepted, allowing lang to
        #    always send the same sequence on replay)."
        # So `accepted` must always be sent on success (whether or not validator ran).
        if update.run_validator
          begin
            # Run validator - no fiber yielding allowed
            @workflow_object._temporal_handle_update_validator(
              update_name,
              input,
              @data_converter
            )
          rescue ex
            # Validator failed - send Rejected response, done
            failure = Internal::FailureConverter.to_failure(ex, @data_converter)
            @context.enqueue_command(
              Coresdk::WorkflowCommands::WorkflowCommand.new(
                update_response: Coresdk::WorkflowCommands::UpdateResponse.new(
                  protocol_instance_id: protocol_instance_id,
                  rejected: failure
                )
              )
            )
            return  # Don't run handler
          end
        end

        # Always send Accepted response (validator passed or was not asked to run).
        @context.enqueue_command(
          Coresdk::WorkflowCommands::WorkflowCommand.new(
            update_response: Coresdk::WorkflowCommands::UpdateResponse.new(
              protocol_instance_id: protocol_instance_id,
              accepted: Google::Protobuf::Empty.new
            )
          )
        )

        # Phase 2: Queue handler for async execution
        @context.queue_update_handler(protocol_instance_id, update_name, input)
      end

      private def activation_completion(
        run_id : String,
        commands : Array(Coresdk::WorkflowCommands::WorkflowCommand)
      ) : Coresdk::WorkflowCompletion::WorkflowActivationCompletion
        Coresdk::WorkflowCompletion::WorkflowActivationCompletion.new(
          run_id: run_id,
          successful: Coresdk::WorkflowCompletion::Success.new(commands: commands)
        )
      end

      private def timestamp_to_time(ts : Google::Protobuf::Timestamp?) : Time
        return Time.utc unless ts
        secs = ts.seconds || 0_i64
        nanos = ts.nanos || 0
        Time.unix(secs) + nanos.nanoseconds
      end

      private def span_to_duration(span : Time::Span?) : Google::Protobuf::Duration?
        return nil if span.nil?
        Google::Protobuf::Duration.new(
          seconds: span.total_seconds.to_i64,
          nanos: span.nanoseconds
        )
      end

      private def proto_duration_to_span(d : Google::Protobuf::Duration?) : Time::Span?
        return nil if d.nil?
        secs = d.seconds || 0_i64
        nanos = d.nanos || 0
        return nil if secs == 0 && nanos == 0
        Time::Span.new(seconds: secs, nanoseconds: nanos)
      end

      private def run_signal_interceptors(
        input : Temporalio::Interceptor::HandleSignalInput,
        &inner : Temporalio::Interceptor::HandleSignalInput -> Nil
      ) : Nil
        chain = @interceptors.reverse
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Temporalio::Interceptor::HandleSignalInput, Nil).new do |i|
            interceptor.handle_signal(i, next_fn)
          end
        end
        fn.call(input)
      end

      private def run_query_interceptors(
        input : Temporalio::Interceptor::HandleQueryInput,
        &inner : Temporalio::Interceptor::HandleQueryInput -> Temporal::Api::Common::V1::Payload?
      ) : Temporal::Api::Common::V1::Payload?
        chain = @interceptors.reverse
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Temporalio::Interceptor::HandleQueryInput, Temporal::Api::Common::V1::Payload?).new do |i|
            interceptor.handle_query(i, next_fn)
          end
        end
        fn.call(input)
      end
    end
  end
end
