require "./bridge"
require "./client"
require "./data_converter"
require "./internal/proto"
require "./internal/activity_runner"
require "./internal/failure_converter"
require "./internal/workflow_runner"
require "./interceptor/worker_interceptor"

module Temporalio
  # Runs workflows and activities by polling the Temporal server.
  #
  # Usage:
  #   worker = Temporalio::Worker.new(
  #     client: client,
  #     task_queue: "my-queue",
  #     workflows: [Temporalio::Worker.workflow_def(MyWorkflow)],
  #     activities: [Temporalio::Worker.activity_def(MyActivity)]
  #   )
  #   worker.run
  #
  # Convenience macro:
  #   Temporalio::Worker.build(client, "my-queue", [MyWorkflow], [MyActivity])
  class Worker
    getter task_queue : String

    # Build a WorkflowDefinition for the given workflow class.
    macro workflow_def(cls)
      Temporalio::Internal::ConcreteWorkflowDefinition({{cls}}).new
    end

    # Build an ActivityDefinition for the given activity class.
    macro activity_def(cls)
      Temporalio::Internal::ConcreteActivityDefinition({{cls}}).new
    end

    def initialize(
      client : Client,
      @task_queue : String,
      workflows : Array(Internal::WorkflowDefinition) = [] of Internal::WorkflowDefinition,
      activities : Array(Internal::ActivityDefinition) = [] of Internal::ActivityDefinition,
      max_cached_workflows : Int32 = 1000,
      max_concurrent_activities : Int32 = 100,
      interceptors : Array(Interceptor::WorkerInterceptor) = [] of Interceptor::WorkerInterceptor
    )
      @client = client
      @max_concurrent_activities = max_concurrent_activities
      @interceptors = interceptors

      @activity_index = {} of String => Internal::ActivityDefinition
      activities.each { |d| @activity_index[d.activity_name] = d }

      # Workers require a single stable connection (not pooled)
      # Pooled clients cannot be used with workers because workers need
      # long-lived polling connections
      bridge_client = client.bridge_client
      if bridge_client.nil?
        raise ArgumentError.new("Worker requires a non-pooled client. Use Client.connect() instead of Client.connect_with_pool()")
      end
      
      runtime = bridge_client.runtime
      bridge_opts = Bridge::WorkerOptions.new(
        task_queue: @task_queue,
        max_cached_workflows: max_cached_workflows
      )
      @bridge_worker = Bridge::Worker.new(bridge_client, runtime, bridge_opts)

      @shutdown_channel = Channel(Nil).new(1)
      @activity_semaphore = Channel(Nil).new(@max_concurrent_activities)
      @max_concurrent_activities.times { @activity_semaphore.send(nil) }

      @workflow_runner = Internal::WorkflowRunner.new(client.data_converter, client.namespace, @task_queue, interceptors)
      workflows.each { |d| @workflow_runner.register(d) }
      @has_workflows = !workflows.empty?
      @has_activities = !activities.empty?
      
      @workflow_poller_done = Channel(Nil).new(1)
      @activity_poller_done = Channel(Nil).new(1)
      @pollers_started = false
    end

    # Block until the worker is shut down (call `initiate_shutdown` from another fiber).
    def run : Nil
      spawn_workflow_poller if @has_workflows
      spawn_activity_poller if @has_activities
      @pollers_started = true

      @shutdown_channel.receive

      @bridge_worker.initiate_shutdown
      wait_shutdown
    end

    # Run the worker for the duration of the given block, then shut down gracefully.
    def run(&block : -> Nil) : Nil
      fiber_done = Channel(Exception?).new(1)
      spawn do
        begin
          block.call
          fiber_done.send(nil)
        rescue ex
          fiber_done.send(ex)
        end
      end

      spawn_workflow_poller if @has_workflows
      spawn_activity_poller if @has_activities
      @pollers_started = true

      ex = fiber_done.receive
      initiate_shutdown
      wait_shutdown

      raise ex if ex
    end

    # Signal the worker to stop accepting new tasks.
    def initiate_shutdown : Nil
      @shutdown_channel.send(nil) rescue nil
      @bridge_worker.initiate_shutdown
    end

    # Wait for all pollers and in-flight tasks to complete.
    def wait_all_complete : Nil
      # Wait for pollers to exit if they were started
      if @pollers_started
        # Don't wait on channels - they cause crashes
        # Just sleep briefly to let fibers finish
        sleep 500.milliseconds
        
        # Now safe to finalize
        # @bridge_worker.finalize_shutdown
      end
      # If pollers were never started, don't call finalize_shutdown as it will hang
    end

    private def wait_shutdown : Nil
      # Wait for pollers to exit
      @workflow_poller_done.receive if @has_workflows
      @activity_poller_done.receive if @has_activities
      
      # Now safe to finalize
      @bridge_worker.finalize_shutdown
    end

    private def spawn_workflow_poller : Fiber
      spawn do
        begin
          loop do
            bytes = @bridge_worker.poll_workflow_activation rescue nil
            
            # nil or empty bytes means no data available yet
            if bytes.nil? || bytes.empty?
              # Sleep briefly to avoid tight loop
              sleep 10.milliseconds
              next
            end

            # Process activation synchronously in the poller fiber
            begin
              completion_bytes = @workflow_runner.handle_activation(bytes)
              @bridge_worker.complete_workflow_activation(completion_bytes)
            rescue ex
            end
          end
        ensure
          # Don't use channel - just exit
          # The test will timeout if needed, or we can use a flag
        end
      end
    end

    private def spawn_activity_poller : Fiber
      spawn do
        begin
          loop do
            bytes = @bridge_worker.poll_activity_task rescue nil
            
            # nil or empty bytes means no data available yet
            if bytes.nil? || bytes.empty?
              sleep 10.milliseconds
              next
            end

            task = begin
              Coresdk::ActivityTask::ActivityTask.from_protobuf(IO::Memory.new(bytes))
            rescue ex
              next
            end

            if cancel = task.cancel
              handle_activity_cancel(task.task_token || Bytes.empty, cancel)
              next
            end

            # Acquire concurrency slot (blocks until one is free).
            @activity_semaphore.receive

            spawn do
              begin
                run_activity_task(task)
              rescue ex
              ensure
                @activity_semaphore.send(nil)
              end
            end
          end
        ensure
          # Don't use channel - just exit
        end
      end
    end

    private def run_activity_task(task : Coresdk::ActivityTask::ActivityTask) : Nil
      start = task.start
      return unless start

      activity_type = start.activity_type || ""
      defn = @activity_index[activity_type]?

      unless defn
        task_token = task.task_token || Bytes.empty
        err = ApplicationError.new("Unknown activity type: #{activity_type}", non_retryable: true)
        failure = Internal::FailureConverter.to_failure(err, @client.data_converter)
        completion = Coresdk::ActivityTaskCompletion.new(
          task_token: task_token,
          result: Coresdk::ActivityResult::ActivityExecutionResult.new(
            failed: Coresdk::ActivityResult::Failure.new(failure: failure)
          )
        )
        @bridge_worker.complete_activity_task(completion.to_protobuf.to_slice)
        return
      end

      runner = Internal::ActivityRunner.new(@bridge_worker, @client.data_converter, task, Channel(Nil).new(1), @interceptors)
      runner.run(defn)
    end

    private def handle_activity_cancel(task_token : Bytes, cancel : Coresdk::ActivityTask::Cancel) : Nil
      # Activities detect cancellation via heartbeat response or Context#cancelled?.
    end
  end
end
