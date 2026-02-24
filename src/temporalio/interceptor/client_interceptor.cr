require "../internal/proto"
require "../data_converter"

module Temporalio
  module Interceptor
    # Input types for client interceptor methods.

    struct StartWorkflowInput
      getter workflow_type : String
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter workflow_id : String
      getter task_queue : String
      getter options : Hash(String, String)

      def initialize(
        @workflow_type : String,
        @args : Array(Temporal::Api::Common::V1::Payload),
        @workflow_id : String,
        @task_queue : String,
        @options : Hash(String, String) = {} of String => String
      )
      end
    end

    struct SignalWorkflowInput
      getter workflow_id : String
      getter run_id : String?
      getter signal : String
      getter args : Array(Temporal::Api::Common::V1::Payload)

      def initialize(@workflow_id : String, @run_id : String?, @signal : String, @args : Array(Temporal::Api::Common::V1::Payload))
      end
    end

    struct QueryWorkflowInput
      getter workflow_id : String
      getter run_id : String?
      getter query_type : String
      getter args : Array(Temporal::Api::Common::V1::Payload)

      def initialize(@workflow_id : String, @run_id : String?, @query_type : String, @args : Array(Temporal::Api::Common::V1::Payload))
      end
    end

    struct CancelWorkflowInput
      getter workflow_id : String
      getter run_id : String?
      getter reason : String?

      def initialize(@workflow_id : String, @run_id : String?, @reason : String? = nil)
      end
    end

    struct TerminateWorkflowInput
      getter workflow_id : String
      getter run_id : String?
      getter reason : String?
      getter details : Array(Temporal::Api::Common::V1::Payload)

      def initialize(
        @workflow_id : String,
        @run_id : String?,
        @reason : String? = nil,
        @details : Array(Temporal::Api::Common::V1::Payload) = [] of Temporal::Api::Common::V1::Payload
      )
      end
    end

    struct DescribeWorkflowInput
      getter workflow_id : String
      getter run_id : String?

      def initialize(@workflow_id : String, @run_id : String? = nil)
      end
    end

    struct ListWorkflowsInput
      getter query : String
      getter page_size : Int32

      def initialize(@query : String = "", @page_size : Int32 = 100)
      end
    end

    struct CountWorkflowsInput
      getter query : String

      def initialize(@query : String = "")
      end
    end

    struct StartUpdateInput
      getter workflow_id : String
      getter run_id : String?
      getter update_name : String
      getter args : Array(Temporal::Api::Common::V1::Payload)
      getter update_id : String
      getter wait_for_stage : Int32

      def initialize(
        @workflow_id : String,
        @run_id : String?,
        @update_name : String,
        @args : Array(Temporal::Api::Common::V1::Payload),
        @update_id : String,
        @wait_for_stage : Int32 = 2
      )
      end
    end

    # Base class for client-side interceptors.
    # Subclass and override only the methods you need; defaults pass through.
    class ClientInterceptor
      def start_workflow(
        input : StartWorkflowInput,
        next_fn : Proc(StartWorkflowInput, String)
      ) : String
        next_fn.call(input)
      end

      def signal_workflow(
        input : SignalWorkflowInput,
        next_fn : Proc(SignalWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      def query_workflow(
        input : QueryWorkflowInput,
        next_fn : Proc(QueryWorkflowInput, Temporal::Api::Common::V1::Payload?)
      ) : Temporal::Api::Common::V1::Payload?
        next_fn.call(input)
      end

      def cancel_workflow(
        input : CancelWorkflowInput,
        next_fn : Proc(CancelWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      def terminate_workflow(
        input : TerminateWorkflowInput,
        next_fn : Proc(TerminateWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      def describe_workflow(
        input : DescribeWorkflowInput,
        next_fn : Proc(DescribeWorkflowInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      def list_workflows(
        input : ListWorkflowsInput,
        next_fn : Proc(ListWorkflowsInput, Nil)
      ) : Nil
        next_fn.call(input)
      end

      def count_workflows(
        input : CountWorkflowsInput,
        next_fn : Proc(CountWorkflowsInput, Int64)
      ) : Int64
        next_fn.call(input)
      end

      def start_update(
        input : StartUpdateInput,
        next_fn : Proc(StartUpdateInput, Nil)
      ) : Nil
        next_fn.call(input)
      end
    end
  end
end
