require "../internal/proto"
require "../internal/failure_converter"
require "../data_converter"
require "./options"

module Temporalio
  class Client
    # A handle to a running or completed workflow execution.
    # Obtained from Client#workflow_handle or Client#start_workflow.
    class WorkflowHandle
      getter workflow_id : String
      getter run_id : String?
      getter result_run_id : String?
      getter first_execution_run_id : String?

      def initialize(
        @client : Client,
        @workflow_id : String,
        @run_id : String? = nil,
        @result_run_id : String? = nil,
        @first_execution_run_id : String? = nil
      )
      end

      # Waits for the workflow to complete and returns the raw JSON result string.
      # Decode with your type: MyType.from_json(result).
      # If the workflow fails, raises the appropriate FailureError.
      # If follow_runs is true (default), follows ContinuedAsNew chains.
      def result(follow_runs : Bool = true) : String?
        run_id = @result_run_id || @run_id
        loop do
          response = fetch_history_close_event(run_id)

          history = response.history
          events = history.try(&.events)
          if events.nil? || events.empty?
            sleep 500.milliseconds
            next
          end

          events.each do |event|
            # WORKAROUND: The event_type field is unreliable in the partial proto binding.
            # Instead, determine the event type by checking which attribute field is set.
            event_type = if event.workflow_execution_completed_event_attributes
              2  # WORKFLOW_EXECUTION_COMPLETED
            elsif event.workflow_execution_failed_event_attributes
              3  # WORKFLOW_EXECUTION_FAILED
            elsif event.workflow_execution_timed_out_event_attributes
              4  # WORKFLOW_EXECUTION_TIMED_OUT
            elsif event.workflow_execution_canceled_event_attributes
              21 # WORKFLOW_EXECUTION_CANCELED
            elsif event.workflow_execution_terminated_event_attributes
              27 # WORKFLOW_EXECUTION_TERMINATED
            elsif event.workflow_execution_continued_as_new_event_attributes
              28 # WORKFLOW_EXECUTION_CONTINUED_AS_NEW
            else
              event.event_type || 0  # Fallback to raw event_type for events we don't handle
            end
            
            case event_type
            when 2 # WORKFLOW_EXECUTION_COMPLETED
              attrs = event.workflow_execution_completed_event_attributes
              if attrs
                values = @client.data_converter.from_payloads_message(attrs.result)
                json_result = values.first?
                # Parse the JSON to get the actual value
                if json_result
                  parsed = JSON.parse(json_result)
                  return parsed.to_s
                end
                return nil
              end
              return nil

            when 3 # WORKFLOW_EXECUTION_FAILED
              attrs = event.workflow_execution_failed_event_attributes
              if attrs && (failure = attrs.failure)
                raise Internal::FailureConverter.from_failure(failure, @client.data_converter)
              end
              raise Error.new("Workflow execution failed")

            when 4 # WORKFLOW_EXECUTION_TIMED_OUT
              attrs = event.workflow_execution_timed_out_event_attributes
              new_run = attrs.try(&.new_execution_run_id)
              if follow_runs && new_run && !new_run.empty?
                run_id = new_run
                break
              end
              raise TimeoutError.new("Workflow execution timed out", timeout_type: 3)

            when 21 # WORKFLOW_EXECUTION_CANCELED
              attrs = event.workflow_execution_canceled_event_attributes
              details = @client.data_converter.from_payloads_message(attrs.try(&.details)).compact
              raise CancelledError.new(details: details)

            when 27 # WORKFLOW_EXECUTION_TERMINATED
              attrs = event.workflow_execution_terminated_event_attributes
              details = @client.data_converter.from_payloads_message(attrs.try(&.details)).compact
              raise TerminatedError.new(
                message: attrs.try(&.reason) || "Terminated",
                details: details
              )

            when 28 # WORKFLOW_EXECUTION_CONTINUED_AS_NEW
              if follow_runs
                attrs = event.workflow_execution_continued_as_new_event_attributes
                new_run = attrs.try(&.new_execution_run_id)
                unless new_run && !new_run.empty?
                  raise Error.new("ContinuedAsNew without new run ID")
                end
                run_id = new_run
                break
              end
              return nil
            
            when 29, 30, 31, 32, 33, 34, 35, 36
              # Skip child workflow events - they don't indicate parent workflow completion
              # 29: START_CHILD_WORKFLOW_EXECUTION_INITIATED
              # 30: START_CHILD_WORKFLOW_EXECUTION_FAILED  
              # 31: CHILD_WORKFLOW_EXECUTION_STARTED
              # 32: CHILD_WORKFLOW_EXECUTION_COMPLETED
              # 33: CHILD_WORKFLOW_EXECUTION_FAILED
              # 34: CHILD_WORKFLOW_EXECUTION_CANCELED
              # 35: CHILD_WORKFLOW_EXECUTION_TIMED_OUT
              # 36: CHILD_WORKFLOW_EXECUTION_TERMINATED
              next
            end
          end

          next if response.next_page_token.try(&.empty?) == false
          
          # Workflow not yet complete, sleep to allow other fibers (like worker) to run
          sleep 500.milliseconds
        end
      end

      # Sends a signal to the workflow.
      # Each arg is encoded via data_converter.to_payload — pass strings, ints,
      # or any JSON::Serializable object.
      def signal(signal_name : String, *args) : Nil
        input_payloads = args.to_a.map { |a| @client.data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
        input = Temporalio::Interceptor::SignalWorkflowInput.new(
          workflow_id: @workflow_id,
          run_id: @run_id,
          signal: signal_name,
          args: input_payloads
        )
        run_interceptors_void(input, @client.interceptors) do |inp|
          req = Temporal::Api::Workflowservice::V1::SignalWorkflowExecutionRequest.new(
            namespace: @client.namespace,
            workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
              workflow_id: inp.workflow_id,
              run_id: inp.run_id || ""
            ),
            signal_name: inp.signal,
            input: inp.args.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: inp.args),
            identity: @client.identity,
            request_id: new_request_id
          )
          @client.workflow_service_call("SignalWorkflowExecution", req.to_protobuf.to_slice)
        end
      end

      # Queries the workflow. Returns the raw JSON result string.
      # Decode with your type: MyType.from_json(result).
      def query(query_type : String, *args, options : QueryOptions? = nil) : String?
        input_payloads = args.to_a.map { |a| @client.data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
        input = Temporalio::Interceptor::QueryWorkflowInput.new(
          workflow_id: @workflow_id,
          run_id: @run_id,
          query_type: query_type,
          args: input_payloads
        )
        result_payload = run_interceptors_query(input, @client.interceptors) do |inp|
          req = Temporal::Api::Workflowservice::V1::QueryWorkflowRequest.new(
            namespace: @client.namespace,
            execution: Temporal::Api::Common::V1::WorkflowExecution.new(
              workflow_id: inp.workflow_id,
              run_id: inp.run_id || ""
            ),
            query: Temporal::Api::Query::V1::WorkflowQuery.new(
              query_type: inp.query_type,
              query_args: inp.args.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: inp.args)
            ),
            query_reject_condition: options.try(&.reject_condition) || 0
          )
          resp_bytes = @client.workflow_service_call("QueryWorkflow", req.to_protobuf.to_slice)
          resp = Temporal::Api::Workflowservice::V1::QueryWorkflowResponse.from_protobuf(IO::Memory.new(resp_bytes))

          if rejected = resp.query_rejected
            raise Error.new("Query rejected: workflow status #{rejected.status}")
          end

          resp.query_result.try { |qr| qr.payloads.try(&.first?) }
        end

        return nil unless result_payload
        values = @client.data_converter.from_payloads_message(
          Temporal::Api::Common::V1::Payloads.new(payloads: [result_payload])
        )
        json_result = values.first?
        if json_result
          parsed = JSON.parse(json_result)
          return parsed.to_s
        end
        nil
      end

      # Executes an update and waits for it to complete.
      # Returns the raw JSON result string.
      # Decode with your type: MyType.from_json(result).
      # If the update is rejected (validator fails or handler fails), raises the appropriate FailureError.
      def execute_update(update_name : String, *args, **options) : String?
        update_id = options[:update_id]?.try(&.to_s) || UUID.random.to_s

        # Convert args to payloads
        input_payloads = args.to_a.map { |a| @client.data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }

        # Build request
        req = Temporal::Api::Workflowservice::V1::UpdateWorkflowExecutionRequest.new(
          namespace: @client.namespace,
          workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
            workflow_id: @workflow_id,
            run_id: @run_id || ""
          ),
          first_execution_run_id: @first_execution_run_id,
          request: Temporal::Api::Update::V1::Request.new(
            meta: Temporal::Api::Update::V1::Meta.new(
              update_id: update_id,
              identity: @client.identity
            ),
            input: Temporal::Api::Update::V1::Input.new(
              name: update_name,
              args: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads)
            )
          ),
          wait_policy: Temporal::Api::Update::V1::WaitPolicy.new(
            lifecycle_stage: 2  # ACCEPTED - wait for validator to pass
          )
        )

        # Send the UpdateWorkflowExecution RPC asynchronously (non-blocking).
        # This is critical: the UpdateWorkflowExecution RPC with lifecycle_stage=ACCEPTED is a
        # long-polling call that blocks until the workflow worker accepts the update.
        # In Crystal's single-threaded fiber model, a blocking call would prevent the worker
        # fiber from running, causing a deadlock. By using the async interface, we yield
        # control back to the scheduler between polls so the worker fiber can process the WFT.
        async_handle = @client.workflow_service_call_async("UpdateWorkflowExecution", req.to_protobuf.to_slice)
        resp_bytes = Bytes.empty
        begin
          loop do
            result = @client.workflow_service_call_poll(async_handle)
            if result
              resp_bytes = result
              break
            end
            # Yield to allow worker fiber to run (process pending WFTs)
            sleep 10.milliseconds
          end
        ensure
          @client.workflow_service_call_free(async_handle)
        end

        resp = Temporal::Api::Workflowservice::V1::UpdateWorkflowExecutionResponse.from_protobuf(IO::Memory.new(resp_bytes))

        # Check if already completed in the response
        if outcome = resp.outcome
          if success = outcome.success
            values = @client.data_converter.from_payloads_message(success)
            return values.first? if values.any?
          elsif failure = outcome.failure
            raise Internal::FailureConverter.from_failure(failure, @client.data_converter)
          end
        end

        # Poll for the result. Give the worker fiber time to run between polls.
        update_handle = UpdateHandle.new(@client, @workflow_id, @run_id, update_id)
        60.times do
          sleep 0.5.seconds
          result = update_handle.poll_result
          return result if result
        end

        nil
      end

      # Starts an update without waiting for completion.
      # Returns an UpdateHandle for async tracking.
      def start_update(update_name : String, *args, **options) : UpdateHandle
        update_id = options[:update_id]?.try(&.to_s) || UUID.random.to_s

        # Convert args to payloads
        input_payloads = args.to_a.map { |a| @client.data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }

        req = Temporal::Api::Workflowservice::V1::UpdateWorkflowExecutionRequest.new(
          namespace: @client.namespace,
          workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
            workflow_id: @workflow_id,
            run_id: @run_id || ""
          ),
          first_execution_run_id: @first_execution_run_id,
          request: Temporal::Api::Update::V1::Request.new(
            meta: Temporal::Api::Update::V1::Meta.new(
              update_id: update_id,
              identity: @client.identity
            ),
            input: Temporal::Api::Update::V1::Input.new(
              name: update_name,
              args: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads)
            )
          ),
          wait_policy: Temporal::Api::Update::V1::WaitPolicy.new(
            lifecycle_stage: 2  # ACCEPTED - wait for validator to pass
          )
        )

        # Send async to avoid deadlock: UpdateWorkflowExecution blocks until the worker accepts
        # the update, but the worker needs the Crystal OS thread to process WFTs.
        async_handle = @client.workflow_service_call_async("UpdateWorkflowExecution", req.to_protobuf.to_slice)
        begin
          loop do
            result = @client.workflow_service_call_poll(async_handle)
            break if result
            # Yield to allow worker fiber to run (process pending WFTs)
            sleep 10.milliseconds
          end
        ensure
          @client.workflow_service_call_free(async_handle)
        end

        # Return UpdateHandle for async tracking
        UpdateHandle.new(@client, @workflow_id, @run_id, update_id)
      end

      # Requests cancellation of the workflow.
      def cancel(reason : String? = nil) : Nil
        input = Temporalio::Interceptor::CancelWorkflowInput.new(
          workflow_id: @workflow_id,
          run_id: @run_id,
          reason: reason
        )
        run_interceptors_cancel(input, @client.interceptors) do |inp|
          req = Temporal::Api::Workflowservice::V1::RequestCancelWorkflowExecutionRequest.new(
            namespace: @client.namespace,
            workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
              workflow_id: inp.workflow_id,
              run_id: inp.run_id || ""
            ),
            identity: @client.identity,
            request_id: new_request_id,
            reason: inp.reason || ""
          )
          @client.workflow_service_call("RequestCancelWorkflowExecution", req.to_protobuf.to_slice)
        end
      end

      # Terminates the workflow.
      # Each detail arg is encoded via data_converter.to_payload.
      def terminate(reason : String? = nil, *details) : Nil
        detail_payloads = details.to_a.map { |d| @client.data_converter.to_payload(d).as(Temporal::Api::Common::V1::Payload) }
        input = Temporalio::Interceptor::TerminateWorkflowInput.new(
          workflow_id: @workflow_id,
          run_id: @run_id,
          reason: reason,
          details: detail_payloads
        )
        run_interceptors_terminate(input, @client.interceptors) do |inp|
          req = Temporal::Api::Workflowservice::V1::TerminateWorkflowExecutionRequest.new(
            namespace: @client.namespace,
            workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
              workflow_id: inp.workflow_id,
              run_id: inp.run_id || ""
            ),
            reason: inp.reason || "",
            details: inp.details.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: inp.details),
            identity: @client.identity
          )
          @client.workflow_service_call("TerminateWorkflowExecution", req.to_protobuf.to_slice)
        end
      end

      # Describes the current state of the workflow execution.
      def describe : WorkflowExecutionDescription
        input = Temporalio::Interceptor::DescribeWorkflowInput.new(
          workflow_id: @workflow_id,
          run_id: @run_id
        )
        # describe_workflow interceptor is a pass-through notification; actual work done inside
        @client.interceptors.each do |i|
          i.describe_workflow(input, Proc(Temporalio::Interceptor::DescribeWorkflowInput, Nil).new { |_| nil })
        end
        req = Temporal::Api::Workflowservice::V1::DescribeWorkflowExecutionRequest.new(
          namespace: @client.namespace,
          execution: Temporal::Api::Common::V1::WorkflowExecution.new(
            workflow_id: @workflow_id,
            run_id: @run_id || ""
          )
        )
        resp_bytes = @client.workflow_service_call("DescribeWorkflowExecution", req.to_protobuf.to_slice)
        resp = Temporal::Api::Workflowservice::V1::DescribeWorkflowExecutionResponse.from_protobuf(IO::Memory.new(resp_bytes))
        info = resp.workflow_execution_info

        WorkflowExecutionDescription.new(
          id: info.try(&.execution).try(&.workflow_id) || @workflow_id,
          run_id: info.try(&.execution).try(&.run_id) || @run_id || "",
          workflow_type: info.try(&.type).try(&.name) || "",
          task_queue: info.try(&.task_queue) || "",
          status: info.try(&.status) || 0,
          start_time: proto_timestamp_to_time(info.try(&.start_time)),
          close_time: proto_timestamp_to_time(info.try(&.close_time)),
          history_length: info.try(&.history_length) || 0_i64,
          history_size_bytes: info.try(&.history_size_bytes) || 0_i64
        )
      end

      # Atomically starts (or reuses) a workflow and sends an update, returning an UpdateHandle.
      # This ensures the workflow is running before the update is processed.
      #
      # The workflow_type, task_queue and args are the same as start_workflow.
      # The start_workflow_operation defines how to start the workflow if it isn't already running.
      #
      # Usage:
      #   op = Temporalio::Client::WithStartWorkflowOperation.new(
      #     workflow_type: "MyWorkflow",
      #     args: ["hello"],
      #     id: "wf-1",
      #     task_queue: "my-queue"
      #   )
      #   update_handle = handle.start_update_with_start("my-update", op)
      #   result = update_handle.result
      def start_update_with_start(
        update_name : String,
        start_operation : WithStartWorkflowOperation,
        *args,
        update_id : String? = nil
      ) : UpdateHandle
        # Start the workflow first (or get existing handle)
        workflow_handle = start_operation.execute(@client)

        # Then start the update on that workflow
        uid = update_id || UUID.random.to_s
        input_payloads = args.to_a.map { |a| @client.data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }

        req = Temporal::Api::Workflowservice::V1::UpdateWorkflowExecutionRequest.new(
          namespace: @client.namespace,
          workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
            workflow_id: workflow_handle.workflow_id,
            run_id: workflow_handle.run_id || ""
          ),
          request: Temporal::Api::Update::V1::Request.new(
            meta: Temporal::Api::Update::V1::Meta.new(
              update_id: uid,
              identity: @client.identity
            ),
            input: Temporal::Api::Update::V1::Input.new(
              name: update_name,
              args: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads)
            )
          ),
          wait_policy: Temporal::Api::Update::V1::WaitPolicy.new(
            lifecycle_stage: 2 # ACCEPTED
          )
        )

        @client.workflow_service_call("UpdateWorkflowExecution", req.to_protobuf.to_slice)
        UpdateHandle.new(@client, workflow_handle.workflow_id, workflow_handle.run_id, uid)
      end

      # Atomically starts (or reuses) a workflow and executes an update, waiting for it to complete.
      def execute_update_with_start(
        update_name : String,
        start_operation : WithStartWorkflowOperation,
        *args,
        update_id : String? = nil
      ) : String?
        handle = start_update_with_start(update_name, start_operation, *args, update_id: update_id)
        handle.result
      end

      private def fetch_history_close_event(run_id : String?) : Temporal::Api::Workflowservice::V1::GetWorkflowExecutionHistoryResponse
        req = Temporal::Api::Workflowservice::V1::GetWorkflowExecutionHistoryRequest.new(
          namespace: @client.namespace,
          execution: Temporal::Api::Common::V1::WorkflowExecution.new(
            workflow_id: @workflow_id,
            run_id: run_id || ""
          ),
          maximum_page_size: 1000,
          wait_new_event: false,  # Don't use long-poll to avoid blocking Crystal fibers
          history_event_filter_type: 1 # ALL_EVENT (was 2 for CLOSE_EVENT)
        )
        resp_bytes = @client.workflow_service_call("GetWorkflowExecutionHistory", req.to_protobuf.to_slice)
        Temporal::Api::Workflowservice::V1::GetWorkflowExecutionHistoryResponse.from_protobuf(IO::Memory.new(resp_bytes))
      end

      private def new_request_id : String
        UUID.random.to_s
      end

      private def proto_timestamp_to_time(ts : Google::Protobuf::Timestamp?) : Time?
        return nil if ts.nil?
        secs = ts.seconds || 0_i64
        nanos = ts.nanos || 0
        Time.unix(secs) + nanos.nanoseconds
      end

      # Run a chain of interceptors for a void signal operation.
      private def run_interceptors_void(
        input : Temporalio::Interceptor::SignalWorkflowInput,
        interceptors : Array(Temporalio::Interceptor::ClientInterceptor),
        &inner : Temporalio::Interceptor::SignalWorkflowInput -> Nil
      ) : Nil
        chain = interceptors.reverse
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Temporalio::Interceptor::SignalWorkflowInput, Nil).new do |i|
            interceptor.signal_workflow(i, next_fn)
          end
        end
        fn.call(input)
      end

      # Run a chain of interceptors for query (returns Payload?).
      private def run_interceptors_query(
        input : Temporalio::Interceptor::QueryWorkflowInput,
        interceptors : Array(Temporalio::Interceptor::ClientInterceptor),
        &inner : Temporalio::Interceptor::QueryWorkflowInput -> Temporal::Api::Common::V1::Payload?
      ) : Temporal::Api::Common::V1::Payload?
        chain = interceptors.reverse
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Temporalio::Interceptor::QueryWorkflowInput, Temporal::Api::Common::V1::Payload?).new do |i|
            interceptor.query_workflow(i, next_fn)
          end
        end
        fn.call(input)
      end

      # Run a chain of interceptors for cancel (void).
      private def run_interceptors_cancel(
        input : Temporalio::Interceptor::CancelWorkflowInput,
        interceptors : Array(Temporalio::Interceptor::ClientInterceptor),
        &inner : Temporalio::Interceptor::CancelWorkflowInput -> Nil
      ) : Nil
        chain = interceptors.reverse
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Temporalio::Interceptor::CancelWorkflowInput, Nil).new do |i|
            interceptor.cancel_workflow(i, next_fn)
          end
        end
        fn.call(input)
      end

      # Run a chain of interceptors for terminate (void).
      private def run_interceptors_terminate(
        input : Temporalio::Interceptor::TerminateWorkflowInput,
        interceptors : Array(Temporalio::Interceptor::ClientInterceptor),
        &inner : Temporalio::Interceptor::TerminateWorkflowInput -> Nil
      ) : Nil
        chain = interceptors.reverse
        fn = chain.reduce(inner) do |next_fn, interceptor|
          Proc(Temporalio::Interceptor::TerminateWorkflowInput, Nil).new do |i|
            interceptor.terminate_workflow(i, next_fn)
          end
        end
        fn.call(input)
      end
    end
  end
end
