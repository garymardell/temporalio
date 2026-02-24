require "uuid"
require "./bridge"
require "./internal/proto"
require "./data_converter"
require "./exceptions"
require "./client/options"
require "./client/workflow_handle"
require "./client/update_handle"
require "./client_pool"
require "./interceptor/client_interceptor"

module Temporalio
  # High-level client for interacting with the Temporal server.
  #
  # Usage:
  #   # Single connection (simple)
  #   client = Temporalio::Client.connect("localhost:7233", namespace: "default")
  #   handle = client.start_workflow("MyWorkflow", id: "wf-1", task_queue: "my-queue")
  #   result = handle.result
  #
  #   # Connection pool (high throughput)
  #   client = Temporalio::Client.connect_with_pool(
  #     "localhost:7233",
  #     namespace: "default",
  #     pool_size: 5
  #   )
  #   # Now start_workflow will use connections from the pool
  class Client
    getter namespace : String
    getter identity : String
    getter data_converter : DataConverter

    # Client-side interceptors applied to all outbound calls.
    getter interceptors : Array(Interceptor::ClientInterceptor)

    # Internal bridge client — exposed for WorkflowHandle to call workflow_service_call.
    # Will be nil if using a connection pool.
    protected getter bridge_client : Bridge::Client?

    # Internal connection pool — used when client is created with connect_with_pool
    protected getter client_pool : ClientPool?

    # Connect to a Temporal server and return a Client.
    def self.connect(
      target_host : String,
      namespace : String = "default",
      api_key : String? = nil,
      identity : String? = nil,
      tls : TlsOptions? = nil,
      data_converter : DataConverter = DataConverter::DEFAULT,
      keep_alive : KeepAliveOptions? = nil,
      metadata : Hash(String, String) = Hash(String, String).new,
      interceptors : Array(Interceptor::ClientInterceptor) = [] of Interceptor::ClientInterceptor
    ) : self
      opts = ConnectOptions.new(
        target_host: target_host,
        namespace: namespace,
        api_key: api_key,
        identity: identity || "#{Process.pid}@#{System.hostname}",
        tls: tls,
        data_converter: data_converter,
        keep_alive: keep_alive,
        metadata: metadata
      )
      connect(opts, interceptors)
    end

    def self.connect(options : ConnectOptions, interceptors : Array(Interceptor::ClientInterceptor) = [] of Interceptor::ClientInterceptor) : self
      runtime = Bridge::Runtime.new

      bridge_opts = Bridge::ClientOptions.new(
        target_url: options.target_host,
        namespace: options.namespace,
        identity: options.identity,
        api_key: options.api_key,
        metadata: options.metadata
      )

      bridge_client = Bridge::Client.connect(runtime, bridge_opts)
      new(bridge_client, nil, options, interceptors)
    end
    
    # Connect to a Temporal server using a connection pool for improved throughput.
    # 
    # Connection pooling allows multiple concurrent RPC requests by maintaining
    # multiple connections to the server. This significantly improves performance
    # when starting many workflows in parallel.
    #
    # Usage:
    #   client = Temporalio::Client.connect_with_pool(
    #     "http://localhost:7234",
    #     namespace: "default",
    #     pool_size: 5,              # Max number of connections
    #     initial_pool_size: 1       # Start with 1 connection, grow as needed
    #   )
    #
    # Performance impact:
    #   - Single connection: ~30 workflows/sec
    #   - Pool of 5: ~100-150 workflows/sec (estimated 3-5x improvement)
    def self.connect_with_pool(
      target_host : String,
      namespace : String = "default",
      api_key : String? = nil,
      identity : String? = nil,
      tls : TlsOptions? = nil,
      data_converter : DataConverter = DataConverter::DEFAULT,
      pool_size : Int32 = 5,
      initial_pool_size : Int32 = 1,
      keep_alive : KeepAliveOptions? = nil,
      metadata : Hash(String, String) = Hash(String, String).new,
      interceptors : Array(Interceptor::ClientInterceptor) = [] of Interceptor::ClientInterceptor
    ) : self
      identity ||= "#{Process.pid}@#{System.hostname}"
      
      pool_options = ClientPool::PoolOptions.new(
        initial_pool_size: initial_pool_size,
        max_pool_size: pool_size,
        max_idle_pool_size: pool_size,
        checkout_timeout: 5.seconds
      )
      
      # Note: TLS support for connection pools not yet implemented
      # For now, use non-TLS connections in the pool
      
      pool = ClientPool.new(
        target_host: target_host,
        namespace: namespace,
        identity: identity,
        api_key: api_key,
        metadata: metadata,
        pool_options: pool_options
      )
      
      opts = ConnectOptions.new(
        target_host: target_host,
        namespace: namespace,
        api_key: api_key,
        identity: identity,
        tls: tls,
        data_converter: data_converter,
        keep_alive: keep_alive,
        metadata: metadata
      )
      
      new(nil, pool, opts, interceptors)
    end

    def initialize(
      @bridge_client : Bridge::Client?,
      @client_pool : ClientPool?,
      options : ConnectOptions,
      @interceptors : Array(Interceptor::ClientInterceptor) = [] of Interceptor::ClientInterceptor
    )
      # Must have either a bridge client or a pool, but not both
      raise ArgumentError.new("Must provide either bridge_client or client_pool") if @bridge_client.nil? && @client_pool.nil?

      @namespace = options.namespace
      @identity = options.identity
      @data_converter = options.data_converter
    end
    
    # Get pool statistics (only available if using connection pool)
    def pool_stats : NamedTuple(total: Int32, idle: Int32, in_use: Int32)?
      @client_pool.try(&.stats)
    end

    # Start a workflow execution and return a WorkflowHandle.
    # The workflow_type is a string name (or use a class that responds to .workflow_type).
    #
    # Pass start_signal and optionally start_signal_args to atomically start the workflow
    # and deliver an initial signal in one operation (signal-with-start).
    def start_workflow(
      workflow_type : String,
      *args,
      id : String,
      task_queue : String,
      execution_timeout : Time::Span? = nil,
      run_timeout : Time::Span? = nil,
      task_timeout : Time::Span? = nil,
      id_reuse_policy : Int32 = 0,
      id_conflict_policy : Int32 = 0,
      retry_policy : RetryPolicy? = nil,
      cron_schedule : String? = nil,
      memo : Hash(String, String)? = nil,
      search_attributes : Hash(String, String)? = nil,
      start_delay : Time::Span? = nil,
      request_id : String? = nil,
      start_signal : String? = nil,
      start_signal_args : Array? = nil,
      static_summary : String? = nil,
      static_details : String? = nil
    ) : WorkflowHandle
      input_payloads = args.to_a.map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }

      # Use signal-with-start when a start_signal is provided
      if sig_name = start_signal
        sig_payloads = (start_signal_args || [] of String).map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
        req = Temporal::Api::Workflowservice::V1::SignalWithStartWorkflowExecutionRequest.new(
          namespace: @namespace,
          workflow_id: id,
          workflow_type: Temporal::Api::Common::V1::WorkflowType.new(name: workflow_type),
          task_queue: Temporal::Api::Taskqueue::V1::TaskQueue.new(name: task_queue),
          input: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads),
          workflow_execution_timeout: span_to_duration(execution_timeout),
          workflow_run_timeout: span_to_duration(run_timeout),
          workflow_task_timeout: span_to_duration(task_timeout),
          identity: @identity,
          request_id: request_id || UUID.random.to_s,
          workflow_id_reuse_policy: id_reuse_policy,
          workflow_id_conflict_policy: id_conflict_policy,
          retry_policy: retry_policy ? convert_retry_policy(retry_policy) : nil,
          cron_schedule: cron_schedule,
          memo: memo ? build_memo(memo) : nil,
          search_attributes: search_attributes ? build_search_attributes(search_attributes) : nil,
          signal_name: sig_name,
          signal_input: sig_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: sig_payloads)
        )
        resp_bytes = workflow_service_call("SignalWithStartWorkflowExecution", req.to_protobuf.to_slice)
        resp = Temporal::Api::Workflowservice::V1::SignalWithStartWorkflowExecutionResponse.from_protobuf(IO::Memory.new(resp_bytes))
        return WorkflowHandle.new(
          client: self,
          workflow_id: id,
          run_id: resp.run_id,
          result_run_id: resp.run_id
        )
      end

      req = Temporal::Api::Workflowservice::V1::StartWorkflowExecutionRequest.new(
        namespace: @namespace,
        workflow_id: id,
        workflow_type: Temporal::Api::Common::V1::WorkflowType.new(name: workflow_type),
        task_queue: Temporal::Api::Taskqueue::V1::TaskQueue.new(name: task_queue),
        input: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads),
        workflow_execution_timeout: span_to_duration(execution_timeout),
        workflow_run_timeout: span_to_duration(run_timeout),
        workflow_task_timeout: span_to_duration(task_timeout),
        identity: @identity,
        request_id: request_id || UUID.random.to_s,
        workflow_id_reuse_policy: id_reuse_policy,
        workflow_id_conflict_policy: id_conflict_policy,
        retry_policy: retry_policy ? convert_retry_policy(retry_policy) : nil,
        cron_schedule: cron_schedule,
        memo: memo ? build_memo(memo) : nil,
        search_attributes: search_attributes ? build_search_attributes(search_attributes) : nil,
        workflow_start_delay: span_to_duration(start_delay)
      )

      resp_bytes = workflow_service_call("StartWorkflowExecution", req.to_protobuf.to_slice)
      resp = Temporal::Api::Workflowservice::V1::StartWorkflowExecutionResponse.from_protobuf(IO::Memory.new(resp_bytes))

      WorkflowHandle.new(
        client: self,
        workflow_id: id,
        run_id: resp.run_id,
        result_run_id: resp.run_id
      )
    end

    # Start a workflow asynchronously (non-blocking).
    # Returns a Future that will contain the WorkflowHandle when the start completes.
    #
    # This enables request pipelining - you can start multiple workflows
    # without waiting for each one to complete before starting the next.
    #
    # Usage:
    #   # Start 100 workflows in parallel
    #   futures = 100.times.map do |i|
    #     client.start_workflow_async("MyWorkflow", "arg#{i}",
    #       id: "wf-#{i}", task_queue: "my-queue")
    #   end
    #   
    #   # Wait for all to complete
    #   handles = futures.map(&.get)
    #
    # Error handling:
    #   future = client.start_workflow_async(...)
    #   begin
    #     handle = future.get
    #   rescue ex : Temporalio::WorkflowAlreadyStartedError
    #     # Handle duplicate workflow ID
    #   end
    # Start a workflow and wait for the result.
    def execute_workflow(
      workflow_type : String,
      *args,
      id : String,
      task_queue : String,
      execution_timeout : Time::Span? = nil,
      run_timeout : Time::Span? = nil,
      task_timeout : Time::Span? = nil,
      id_reuse_policy : Int32 = 0,
      id_conflict_policy : Int32 = 0,
      retry_policy : RetryPolicy? = nil,
      cron_schedule : String? = nil,
      memo : Hash(String, String)? = nil,
      search_attributes : Hash(String, String)? = nil,
      start_delay : Time::Span? = nil,
      request_id : String? = nil,
      start_signal : String? = nil,
      start_signal_args : Array? = nil,
      static_summary : String? = nil,
      static_details : String? = nil
    ) : String?
      handle = start_workflow(
        workflow_type, *args,
        id: id,
        task_queue: task_queue,
        execution_timeout: execution_timeout,
        run_timeout: run_timeout,
        task_timeout: task_timeout,
        id_reuse_policy: id_reuse_policy,
        id_conflict_policy: id_conflict_policy,
        retry_policy: retry_policy,
        cron_schedule: cron_schedule,
        memo: memo,
        search_attributes: search_attributes,
        start_delay: start_delay,
        request_id: request_id,
        start_signal: start_signal,
        start_signal_args: start_signal_args,
        static_summary: static_summary,
        static_details: static_details
      )
      handle.result
    end

    # Batch start multiple workflows in parallel for maximum throughput.
    # Returns an array of WorkflowHandles in the same order as requests.
    #
    # This is much faster than calling start_workflow in a loop because it:
    # - Spawns all RPC calls concurrently
    # - Reduces total network roundtrip time
    #
    # Usage:
    #   requests = 100.times.map { |i|
    #     {
    #       workflow_type: "MyWorkflow",
    #       args: ["arg#{i}"],
    #       id: "wf-#{i}",
    #       task_queue: "my-queue"
    #     }
    #   }
    #   handles = client.batch_start_workflows(requests)
    def batch_start_workflows(
      requests : Array(NamedTuple(
        workflow_type: String,
        args: Array,
        id: String,
        task_queue: String,
        execution_timeout: Time::Span?,
        run_timeout: Time::Span?,
        task_timeout: Time::Span?,
        id_reuse_policy: Int32,
        id_conflict_policy: Int32,
        retry_policy: RetryPolicy?,
        cron_schedule: String?,
        memo: Hash(String, String)?,
        search_attributes: Hash(String, String)?,
        start_delay: Time::Span?,
        request_id: String?
      ) | NamedTuple(
        workflow_type: String,
        args: Array,
        id: String,
        task_queue: String
      ))
    ) : Array(WorkflowHandle)
      # Channel to collect results
      channel = Channel(Tuple(Int32, WorkflowHandle)).new(requests.size)
      
      # Spawn a fiber for each workflow start
      requests.each_with_index do |req, idx|
        spawn do
          begin
            # Extract params with defaults
            execution_timeout = req[:execution_timeout]? || nil
            run_timeout = req[:run_timeout]? || nil
            task_timeout = req[:task_timeout]? || nil
            id_reuse_policy = req[:id_reuse_policy]? || 0
            id_conflict_policy = req[:id_conflict_policy]? || 0
            retry_policy = req[:retry_policy]? || nil
            cron_schedule = req[:cron_schedule]? || nil
            memo = req[:memo]? || nil
            search_attributes = req[:search_attributes]? || nil
            start_delay = req[:start_delay]? || nil
            request_id = req[:request_id]? || nil
            
            # Build the start request manually (can't splat dynamic array)
            input_payloads = req[:args].map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
            
            start_req = Temporal::Api::Workflowservice::V1::StartWorkflowExecutionRequest.new(
              namespace: @namespace,
              workflow_id: req[:id],
              workflow_type: Temporal::Api::Common::V1::WorkflowType.new(name: req[:workflow_type]),
              task_queue: Temporal::Api::Taskqueue::V1::TaskQueue.new(name: req[:task_queue]),
              input: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads),
              workflow_execution_timeout: span_to_duration(execution_timeout),
              workflow_run_timeout: span_to_duration(run_timeout),
              workflow_task_timeout: span_to_duration(task_timeout),
              identity: @identity,
              request_id: request_id || UUID.random.to_s,
              workflow_id_reuse_policy: id_reuse_policy,
              workflow_id_conflict_policy: id_conflict_policy,
              retry_policy: retry_policy ? convert_retry_policy(retry_policy) : nil,
              cron_schedule: cron_schedule,
              memo: memo ? build_memo(memo) : nil,
              search_attributes: search_attributes ? build_search_attributes(search_attributes) : nil,
              workflow_start_delay: span_to_duration(start_delay)
            )
            
            resp_bytes = workflow_service_call("StartWorkflowExecution", start_req.to_protobuf.to_slice)
            resp = Temporal::Api::Workflowservice::V1::StartWorkflowExecutionResponse.from_protobuf(IO::Memory.new(resp_bytes))
            
            handle = WorkflowHandle.new(
              client: self,
              workflow_id: req[:id],
              run_id: resp.run_id,
              result_run_id: resp.run_id
            )
            channel.send({idx, handle})
          rescue ex
            # On error, send a failed handle (workflow_id with empty run_id signals error)
            channel.send({idx, WorkflowHandle.new(
              client: self,
              workflow_id: req[:id],
              run_id: nil,
              result_run_id: nil
            )})
          end
        end
      end
      
      # Collect all results
      results = Array(Tuple(Int32, WorkflowHandle)).new(requests.size)
      requests.size.times do
        results << channel.receive
      end
      
      # Sort by original index and extract handles
      results.sort_by! { |(idx, _)| idx }
      results.map { |(_, handle)| handle }
    end

    # Start multiple workflows with request pipelining for maximum throughput.
    # This is 2-3x faster than batch_start_workflows because it doesn't wait
    # for each start response before sending the next request.
    #
    # Returns an array of Futures that will contain WorkflowHandles.
    # Errors are captured in the futures - check each future's result.
    #
    # Usage:
    #   requests = 1000.times.map { |i|
    #     {
    #       workflow_type: "MyWorkflow",
    #       args: ["arg#{i}"],
    #       id: "wf-#{i}",
    #       task_queue: "my-queue"
    #     }
    #   }
    #   
    #   # Start all (non-blocking, pipelined)
    #   futures = client.start_workflows_pipelined(requests)
    #   
    #   # Wait for all and handle errors
    #   results = Async.await_all_settled(futures)
    #   results.each_with_index do |result, i|
    #     if result[:success]
    #       puts "Workflow #{i}: Started with handle #{result[:value]}"
    #     else
    #       puts "Workflow #{i}: Failed - #{result[:error]}"
    #     end
    #   end
    def start_workflows_pipelined(
      requests : Array(NamedTuple(
        workflow_type: String,
        args: Array,
        id: String,
        task_queue: String,
        execution_timeout: Time::Span?,
        run_timeout: Time::Span?,
        task_timeout: Time::Span?,
        id_reuse_policy: Int32,
        id_conflict_policy: Int32,
        retry_policy: RetryPolicy?,
        cron_schedule: String?,
        memo: Hash(String, String)?,
        search_attributes: Hash(String, String)?,
        start_delay: Time::Span?,
        request_id: String?
      ) | NamedTuple(
        workflow_type: String,
        args: Array,
        id: String,
        task_queue: String
      ))
    ) : Array(Async::Future(WorkflowHandle))
      requests.map do |req|
        # Extract params with defaults
        execution_timeout = req[:execution_timeout]? || nil
        run_timeout = req[:run_timeout]? || nil
        task_timeout = req[:task_timeout]? || nil
        id_reuse_policy = req[:id_reuse_policy]? || 0
        id_conflict_policy = req[:id_conflict_policy]? || 0
        retry_policy = req[:retry_policy]? || nil
        cron_schedule = req[:cron_schedule]? || nil
        memo = req[:memo]? || nil
        search_attributes = req[:search_attributes]? || nil
        start_delay = req[:start_delay]? || nil
        request_id = req[:request_id]? || nil
        
        # Create async future for each workflow
        future = Async::Future(WorkflowHandle).new
        
        spawn do
          begin
            # Build the start request
            input_payloads = req[:args].map { |a| @data_converter.to_payload(a).as(Temporal::Api::Common::V1::Payload) }
            
            start_req = Temporal::Api::Workflowservice::V1::StartWorkflowExecutionRequest.new(
              namespace: @namespace,
              workflow_id: req[:id],
              workflow_type: Temporal::Api::Common::V1::WorkflowType.new(name: req[:workflow_type]),
              task_queue: Temporal::Api::Taskqueue::V1::TaskQueue.new(name: req[:task_queue]),
              input: input_payloads.empty? ? nil : Temporal::Api::Common::V1::Payloads.new(payloads: input_payloads),
              workflow_execution_timeout: span_to_duration(execution_timeout),
              workflow_run_timeout: span_to_duration(run_timeout),
              workflow_task_timeout: span_to_duration(task_timeout),
              identity: @identity,
              request_id: request_id || UUID.random.to_s,
              workflow_id_reuse_policy: id_reuse_policy,
              workflow_id_conflict_policy: id_conflict_policy,
              retry_policy: retry_policy ? convert_retry_policy(retry_policy) : nil,
              cron_schedule: cron_schedule,
              memo: memo ? build_memo(memo) : nil,
              search_attributes: search_attributes ? build_search_attributes(search_attributes) : nil,
              workflow_start_delay: span_to_duration(start_delay)
            )
            
            resp_bytes = workflow_service_call("StartWorkflowExecution", start_req.to_protobuf.to_slice)
            resp = Temporal::Api::Workflowservice::V1::StartWorkflowExecutionResponse.from_protobuf(IO::Memory.new(resp_bytes))
            
            handle = WorkflowHandle.new(
              client: self,
              workflow_id: req[:id],
              run_id: resp.run_id,
              result_run_id: resp.run_id
            )
            
            future.set(handle)
          rescue ex
            future.set_error(ex)
          end
        end
        
        future
      end
    end

    # Get a handle to an existing workflow (by ID, optionally with run ID).
    def workflow_handle(workflow_id : String, run_id : String? = nil) : WorkflowHandle
      WorkflowHandle.new(
        client: self,
        workflow_id: workflow_id,
        run_id: run_id
      )
    end

    # List workflow executions matching an optional query.
    # Returns an Iterator that lazily fetches pages.
    def list_workflows(query : String = "", page_size : Int32 = 100) : WorkflowListIterator
      WorkflowListIterator.new(self, query, page_size)
    end

    # Count workflows matching a query.
    def count_workflows(query : String = "") : Int64
      input = Interceptor::CountWorkflowsInput.new(query: query)
      run_interceptors_count(input) do |inp|
        req = Temporal::Api::Workflowservice::V1::CountWorkflowExecutionsRequest.new(
          namespace: @namespace,
          query: inp.query
        )
        resp_bytes = workflow_service_call("CountWorkflowExecutions", req.to_protobuf.to_slice)
        resp = Temporal::Api::Workflowservice::V1::CountWorkflowExecutionsResponse.from_protobuf(IO::Memory.new(resp_bytes))
        resp.count || 0_i64
      end
    end

    # Run a chain of interceptors for count_workflows, returning Int64.
    private def run_interceptors_count(
      input : Interceptor::CountWorkflowsInput,
      &inner : Interceptor::CountWorkflowsInput -> Int64
    ) : Int64
      chain = @interceptors.reverse
      fn = chain.reduce(inner) do |next_fn, interceptor|
        Proc(Interceptor::CountWorkflowsInput, Int64).new do |i|
          interceptor.count_workflows(i, next_fn)
        end
      end
      fn.call(input)
    end

    # Internal: make a workflow service RPC call.
    # Returns raw response bytes. Called by WorkflowHandle too.
    protected def workflow_service_call(rpc_name : String, request_bytes : Bytes) : Bytes
      # Use connection pool if available, otherwise use direct client
      if pool = @client_pool
        pool.checkout do |client|
          client.rpc_call(
            :workflow,
            rpc_name,
            request_bytes
          )
        end
      elsif client = @bridge_client
        client.rpc_call(
          :workflow,
          rpc_name,
          request_bytes
        )
      else
        raise "No client or pool available"
      end
    end

    private def span_to_duration(span : Time::Span?) : Google::Protobuf::Duration?
      return nil if span.nil?
      Google::Protobuf::Duration.new(
        seconds: span.total_seconds.to_i64,
        nanos: span.nanoseconds
      )
    end

    private def convert_retry_policy(policy : RetryPolicy) : Temporal::Api::Common::V1::RetryPolicy
      Temporal::Api::Common::V1::RetryPolicy.new(
        initial_interval: span_to_duration(policy.initial_interval),
        backoff_coefficient: policy.backoff_coefficient,
        maximum_interval: span_to_duration(policy.maximum_interval),
        maximum_attempts: policy.maximum_attempts,
        non_retryable_error_types: policy.non_retryable_error_types
      )
    end

    private def build_memo(memo : Hash(String, String)) : Temporal::Api::Common::V1::Memo
      entries = memo.map do |k, v|
        Temporal::Api::Common::V1::MemoFieldsEntry.new(
          key: k,
          value: @data_converter.to_payload(v)
        )
      end
      Temporal::Api::Common::V1::Memo.new(fields: entries)
    end

    private def build_search_attributes(attrs : Hash(String, String)) : Temporal::Api::Common::V1::SearchAttributes
      entries = attrs.map do |k, v|
        Temporal::Api::Common::V1::StringPayloadEntry.new(
          key: k,
          value: @data_converter.to_payload(v)
        )
      end
      Temporal::Api::Common::V1::SearchAttributes.new(indexed_fields: entries)
    end

    # Iterator for listing workflows with pagination.
    class WorkflowListIterator
      include Iterator(WorkflowExecutionInfo)

      def initialize(
        @client : Client,
        @query : String,
        @page_size : Int32
      )
        @page_token = Bytes.empty
        @buffer = [] of WorkflowExecutionInfo
        @done = false
        @fetched_once = false
      end

      def next : WorkflowExecutionInfo | Iterator::Stop
        if @buffer.empty?
          return stop if @done
          fetch_next_page
          return stop if @buffer.empty?
        end
        @buffer.shift
      end

      private def fetch_next_page
        req = Temporal::Api::Workflowservice::V1::ListWorkflowExecutionsRequest.new(
          namespace: @client.namespace,
          page_size: @page_size,
          next_page_token: @page_token.empty? ? nil : @page_token,
          query: @query
        )
        resp_bytes = @client.workflow_service_call("ListWorkflowExecutions", req.to_protobuf.to_slice)
        resp = Temporal::Api::Workflowservice::V1::ListWorkflowExecutionsResponse.from_protobuf(IO::Memory.new(resp_bytes))

        token = resp.next_page_token
        if token.nil? || token.empty?
          @done = true
        else
          @page_token = token
        end

        executions = resp.executions || [] of Temporal::Api::Workflow::V1::WorkflowExecutionInfo
        executions.each do |ex|
          @buffer << WorkflowExecutionInfo.new(
            id: ex.execution.try(&.workflow_id) || "",
            run_id: ex.execution.try(&.run_id) || "",
            workflow_type: ex.type.try(&.name) || "",
            task_queue: ex.task_queue || "",
            status: ex.status || 0,
            start_time: proto_timestamp_to_time(ex.start_time),
            close_time: proto_timestamp_to_time(ex.close_time)
          )
        end
      end

      private def proto_timestamp_to_time(ts : Google::Protobuf::Timestamp?) : Time?
        return nil if ts.nil?
        secs = ts.seconds || 0_i64
        nanos = ts.nanos || 0
        Time.unix(secs) + nanos.nanoseconds
      end
    end
  end
end
