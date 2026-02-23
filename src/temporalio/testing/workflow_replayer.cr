require "../bridge"
require "../data_converter"
require "../internal/proto"
require "../internal/workflow_runner"

module Temporalio
  module Testing
    # Replays recorded workflow histories to detect nondeterminism errors.
    #
    # Usage:
    #   replayer = Temporalio::Testing::WorkflowReplayer.new(
    #     workflows: [Worker.workflow_def(MyWorkflow)]
    #   )
    #   replayer.replay_workflow(history_json_string)
    class WorkflowReplayer
      def initialize(
        workflows : Array(Internal::WorkflowDefinition),
        @data_converter : DataConverter = DataConverter::DEFAULT
      )
        @runner = Internal::WorkflowRunner.new(@data_converter)
        workflows.each { |d| @runner.register(d) }
        @runtime = Bridge::Runtime.new
      end

      # Replay a workflow history provided as a JSON string.
      # Raises NondeterminismError if the replay diverges from the recorded history.
      def replay_workflow(history_json : String) : Nil
        replay_bytes = history_json.to_slice

        channel = Channel(Exception?).new(1)

        callback = TemporalCore::TemporalCoreWorkerReplayPushCallback.new do |userdata, error_ptr|
          box = Box(typeof(channel)).unbox(userdata)
          if error_ptr
            msg = String.new(error_ptr.value.data.as(UInt8*), error_ptr.value.size)
            if msg.includes?("nondeterminism") || msg.includes?("Nondeterminism")
              box.send(NondeterminismError.new(msg))
            else
              box.send(Exception.new(msg))
            end
          else
            box.send(nil)
          end
        end

        # Build a replayer worker using Core's replayer API.
        replayer_channel = Channel(Tuple(TemporalCore::TemporalCoreWorkerReplayer*, Exception?)).new(1)

        replayer_create_cb = TemporalCore::TemporalCoreWorkerReplayerNewCallback.new do |userdata, replayer_ptr, error_ptr|
          box = Box(typeof(replayer_channel)).unbox(userdata)
          if error_ptr
            msg = String.new(error_ptr.value.data.as(UInt8*), error_ptr.value.size)
            box.send({Pointer(TemporalCore::TemporalCoreWorkerReplayer).null, Exception.new(msg)})
          else
            box.send({replayer_ptr, nil})
          end
        end

        replayer_box = Box.new(replayer_channel)
        TemporalCore.temporal_core_worker_replayer_new(
          @runtime.ptr,
          replayer_box.as(Void*),
          replayer_create_cb
        )
        replayer_ptr, create_err = replayer_channel.receive
        raise create_err if create_err

        push_box = Box.new(channel)
        history_ref = TemporalCore::TemporalCoreByteArray.new
        history_ref.data = replay_bytes.to_unsafe.as(UInt8*)
        history_ref.size = replay_bytes.size.to_u64

        TemporalCore.temporal_core_worker_replay_push(
          replayer_ptr,
          pointerof(history_ref),
          push_box.as(Void*),
          callback
        )
        result = channel.receive

        TemporalCore.temporal_core_worker_replayer_free(replayer_ptr)

        raise result if result
      end
    end
  end
end
