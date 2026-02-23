require "../activity"
require "../activity/context"
require "../activity/activity_info"
require "../data_converter"
require "../internal/proto"

module Temporalio
  module Testing
    # Unit test environment for activities — runs activities without a server.
    #
    # Usage:
    #   env = Temporalio::Testing::ActivityEnvironment.new
    #   dc  = Temporalio::DataConverter::DEFAULT
    #   result = env.run(MyActivity.new, dc.to_payload("arg"))
    #   decoded = dc.from_payload(result.not_nil!, String)
    #
    # Capture heartbeats (payloads):
    #   beats = [] of Array(Temporal::Api::Common::V1::Payload)
    #   env = Temporalio::Testing::ActivityEnvironment.new(
    #     on_heartbeat: ->(details : Array(Temporal::Api::Common::V1::Payload)) { beats << details }
    #   )
    class ActivityEnvironment
      getter info : Activity::ActivityInfo
      getter? cancelled : Bool = false
      getter? worker_shutdown : Bool = false

      def initialize(
        workflow_id : String = "test-workflow-id",
        workflow_run_id : String = "test-run-id",
        workflow_namespace : String = "default",
        workflow_type : String = "TestWorkflow",
        activity_id : String = "test-activity-id",
        activity_type : String = "TestActivity",
        task_queue : String = "test-task-queue",
        attempt : Int32 = 1,
        @data_converter : DataConverter = DataConverter::DEFAULT,
        on_heartbeat : Proc(Array(Temporal::Api::Common::V1::Payload), Nil)? = nil
      )
        @on_heartbeat = on_heartbeat

        task_token = Bytes.new(16) { |i| i.to_u8 }

        @info = Activity::ActivityInfo.new(
          workflow_id: workflow_id,
          workflow_run_id: workflow_run_id,
          workflow_namespace: workflow_namespace,
          workflow_type: workflow_type,
          activity_id: activity_id,
          activity_type: activity_type,
          task_queue: task_queue,
          task_token: task_token,
          attempt: attempt,
          scheduled_time: Time.utc,
          started_time: Time.utc,
          deadline: Time.utc + 10.minutes,
          heartbeat_details: [] of Temporal::Api::Common::V1::Payload,
          schedule_to_close_timeout: nil,
          start_to_close_timeout: nil,
          heartbeat_timeout: nil,
          is_local: false
        )
      end

      # Run an activity instance with pre-encoded Payload arguments.
      # Use data_converter.to_payload(value) to encode each argument.
      # Returns the result Payload (decode with data_converter.from_payload(result, MyType)).
      def run(activity : T, *args : Temporal::Api::Common::V1::Payload) : Temporal::Api::Common::V1::Payload? forall T
        heartbeat_proc = Proc(Array(Temporal::Api::Common::V1::Payload), Nil).new do |details|
          @on_heartbeat.try(&.call(details))
        end

        context = Activity::Context.new(@info, heartbeat_proc)
        context.request_cancel! if @cancelled
        context.notify_worker_shutdown! if @worker_shutdown

        context.install!
        begin
          activity._temporal_execute(args.to_a, @data_converter)
        ensure
          context.uninstall!
        end
      end

      # Simulate cancellation of the activity.
      def cancel! : Nil
        @cancelled = true
      end

      # Simulate worker shutdown notification.
      def simulate_worker_shutdown! : Nil
        @worker_shutdown = true
      end
    end
  end
end
