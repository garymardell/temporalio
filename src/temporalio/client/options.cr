require "../data_converter"

module Temporalio
  class Client
    # Options for connecting a Temporalio::Client to a Temporal server.
    class ConnectOptions
      getter target_host : String
      getter namespace : String
      getter api_key : String?
      getter identity : String
      getter tls : TlsOptions?
      getter data_converter : DataConverter
      getter keep_alive : KeepAliveOptions?
      getter metadata : Hash(String, String)

      def initialize(
        @target_host : String,
        @namespace : String = "default",
        @api_key : String? = nil,
        @identity : String = "#{Process.pid}@#{System.hostname}",
        @tls : TlsOptions? = nil,
        @data_converter : DataConverter = DataConverter::DEFAULT,
        @keep_alive : KeepAliveOptions? = nil,
        @metadata : Hash(String, String) = Hash(String, String).new
      )
      end
    end

    class TlsOptions
      getter server_root_ca_cert : Bytes?
      getter server_name_override : String?
      getter client_cert : Bytes?
      getter client_private_key : Bytes?

      def initialize(
        @server_root_ca_cert : Bytes? = nil,
        @server_name_override : String? = nil,
        @client_cert : Bytes? = nil,
        @client_private_key : Bytes? = nil
      )
      end
    end

    class KeepAliveOptions
      getter interval : Time::Span
      getter timeout : Time::Span

      def initialize(
        @interval : Time::Span = 30.seconds,
        @timeout : Time::Span = 15.seconds
      )
      end
    end

    # Options for starting a workflow.
    class StartWorkflowOptions
      getter id : String
      getter task_queue : String
      getter execution_timeout : Time::Span?
      getter run_timeout : Time::Span?
      getter task_timeout : Time::Span?
      getter id_reuse_policy : Int32
      getter id_conflict_policy : Int32
      getter retry_policy : RetryPolicy?
      getter cron_schedule : String?
      getter memo : Hash(String, String)?
      getter search_attributes : Hash(String, String)?
      getter start_delay : Time::Span?

      def initialize(
        @id : String,
        @task_queue : String,
        @execution_timeout : Time::Span? = nil,
        @run_timeout : Time::Span? = nil,
        @task_timeout : Time::Span? = nil,
        @id_reuse_policy : Int32 = 0,
        @id_conflict_policy : Int32 = 0,
        @retry_policy : RetryPolicy? = nil,
        @cron_schedule : String? = nil,
        @memo : Hash(String, String)? = nil,
        @search_attributes : Hash(String, String)? = nil,
        @start_delay : Time::Span? = nil
      )
      end
    end

    # Mirrors temporal.api.common.v1.RetryPolicy
    class RetryPolicy
      getter initial_interval : Time::Span
      getter backoff_coefficient : Float64
      getter maximum_interval : Time::Span?
      getter maximum_attempts : Int32
      getter non_retryable_error_types : Array(String)

      def initialize(
        @initial_interval : Time::Span = 1.second,
        @backoff_coefficient : Float64 = 2.0,
        @maximum_interval : Time::Span? = nil,
        @maximum_attempts : Int32 = 0,
        @non_retryable_error_types : Array(String) = [] of String
      )
      end
    end

    # Options for querying a workflow.
    class QueryOptions
      getter reject_condition : Int32

      def initialize(@reject_condition : Int32 = 0)
      end
    end

    # Description returned by WorkflowHandle#describe.
    class WorkflowExecutionDescription
      getter id : String
      getter run_id : String
      getter workflow_type : String
      getter task_queue : String
      getter status : Int32
      getter start_time : Time?
      getter close_time : Time?
      getter history_length : Int64
      getter history_size_bytes : Int64

      def initialize(
        @id : String,
        @run_id : String,
        @workflow_type : String,
        @task_queue : String,
        @status : Int32,
        @start_time : Time?,
        @close_time : Time?,
        @history_length : Int64,
        @history_size_bytes : Int64
      )
      end

      def status_name : String
        case @status
        when 0 then "UNSPECIFIED"
        when 1 then "RUNNING"
        when 2 then "COMPLETED"
        when 3 then "FAILED"
        when 4 then "CANCELED"
        when 5 then "TERMINATED"
        when 6 then "CONTINUED_AS_NEW"
        when 7 then "TIMED_OUT"
        else        "UNKNOWN(#{@status})"
        end
      end
    end

    # Basic workflow execution info for list results.
    class WorkflowExecutionInfo
      getter id : String
      getter run_id : String
      getter workflow_type : String
      getter task_queue : String
      getter status : Int32
      getter start_time : Time?
      getter close_time : Time?

      def initialize(
        @id : String,
        @run_id : String,
        @workflow_type : String,
        @task_queue : String,
        @status : Int32,
        @start_time : Time?,
        @close_time : Time?
      )
      end
    end
  end
end
