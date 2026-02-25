module Temporalio
  class Client
    class UpdateHandle
      getter update_id : String
      getter workflow_id : String
      getter run_id : String?

      def initialize(
        @client : Client,
        @workflow_id : String,
        @run_id : String?,
        @update_id : String
      )
      end

      # Wait for and return result. Polls until the update completes or fails.
      # Raises on update rejection or failure.
      def result : String?
        60.times do
          req = Temporal::Api::Workflowservice::V1::PollWorkflowExecutionUpdateRequest.new(
            namespace: @client.namespace,
            update_ref: Temporal::Api::Update::V1::UpdateRef.new(
              workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
                workflow_id: @workflow_id,
                run_id: @run_id || ""
              ),
              update_id: @update_id
            )
          )

          resp_bytes = @client.workflow_service_call("PollWorkflowExecutionUpdate", req.to_protobuf.to_slice)
          resp = Temporal::Api::Workflowservice::V1::PollWorkflowExecutionUpdateResponse.from_protobuf(IO::Memory.new(resp_bytes))

          # Extract outcome
          if outcome = resp.outcome
            if success = outcome.success
              values = @client.data_converter.from_payloads_message(success)
              return values.first? if values.any?
              return nil
            elsif failure = outcome.failure
              raise Internal::FailureConverter.from_failure(failure, @client.data_converter)
            end
          end

          # Not yet complete, give the worker fiber time to run
          sleep 500.milliseconds
        end

        nil
      end

      # Poll once for the result without blocking. Returns nil if not yet complete.
      # Raises on update rejection or failure.
      def poll_result : String?
        req = Temporal::Api::Workflowservice::V1::PollWorkflowExecutionUpdateRequest.new(
          namespace: @client.namespace,
          update_ref: Temporal::Api::Update::V1::UpdateRef.new(
            workflow_execution: Temporal::Api::Common::V1::WorkflowExecution.new(
              workflow_id: @workflow_id,
              run_id: @run_id || ""
            ),
            update_id: @update_id
          )
        )

        resp_bytes = @client.workflow_service_call("PollWorkflowExecutionUpdate", req.to_protobuf.to_slice)
        resp = Temporal::Api::Workflowservice::V1::PollWorkflowExecutionUpdateResponse.from_protobuf(IO::Memory.new(resp_bytes))

        if outcome = resp.outcome
          if success = outcome.success
            values = @client.data_converter.from_payloads_message(success)
            return values.first? if values.any?
          elsif failure = outcome.failure
            raise Internal::FailureConverter.from_failure(failure, @client.data_converter)
          end
        end

        nil
      end
    end
  end
end
