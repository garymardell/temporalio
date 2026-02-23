require "../internal/proto"
require "../data_converter"

module Temporalio
  module Activity
    # Contextual information about the currently executing activity task.
    class ActivityInfo
      getter workflow_id : String
      getter workflow_run_id : String
      getter workflow_namespace : String
      getter workflow_type : String
      getter activity_id : String
      getter activity_type : String
      getter task_queue : String
      getter task_token : Bytes
      getter attempt : Int32
      getter scheduled_time : Time
      getter started_time : Time
      getter deadline : Time
      # Heartbeat details from the previous attempt. Decode each with dc.from_payload(p, MyType).
      getter heartbeat_details : Array(Temporal::Api::Common::V1::Payload)
      getter schedule_to_close_timeout : Time::Span?
      getter start_to_close_timeout : Time::Span?
      getter heartbeat_timeout : Time::Span?
      getter is_local : Bool

      def initialize(
        @workflow_id : String,
        @workflow_run_id : String,
        @workflow_namespace : String,
        @workflow_type : String,
        @activity_id : String,
        @activity_type : String,
        @task_queue : String,
        @task_token : Bytes,
        @attempt : Int32,
        @scheduled_time : Time,
        @started_time : Time,
        @deadline : Time,
        @heartbeat_details : Array(Temporal::Api::Common::V1::Payload),
        @schedule_to_close_timeout : Time::Span? = nil,
        @start_to_close_timeout : Time::Span? = nil,
        @heartbeat_timeout : Time::Span? = nil,
        @is_local : Bool = false
      )
      end

      # Build from a coresdk ActivityTask::Start proto message.
      def self.from_proto(
        task_token : Bytes,
        start : Coresdk::ActivityTask::Start,
        converter : DataConverter = DataConverter::DEFAULT
      ) : ActivityInfo
        wf_exec = start.workflow_execution
        hb_details = start.heartbeat_details || [] of Temporal::Api::Common::V1::Payload

        scheduled_time = proto_timestamp_to_time(start.current_attempt_scheduled_time) || Time.utc
        started_time = proto_timestamp_to_time(start.started_time) || Time.utc

        # Deadline: min of schedule_to_close (from scheduled) and start_to_close (from started)
        deadline = calculate_deadline(scheduled_time, started_time, start)

        new(
          workflow_id: wf_exec.try(&.workflow_id) || "",
          workflow_run_id: wf_exec.try(&.run_id) || "",
          workflow_namespace: start.workflow_namespace || "",
          workflow_type: start.workflow_type || "",
          activity_id: start.activity_id || "",
          activity_type: start.activity_type || "",
          task_queue: "",  # not in Start proto, comes from worker config
          task_token: task_token,
          attempt: (start.attempt || 1_u32).to_i32,
          scheduled_time: scheduled_time,
          started_time: started_time,
          deadline: deadline,
          heartbeat_details: hb_details,
          schedule_to_close_timeout: proto_duration_to_span(start.schedule_to_close_timeout),
          start_to_close_timeout: proto_duration_to_span(start.start_to_close_timeout),
          heartbeat_timeout: proto_duration_to_span(start.heartbeat_timeout),
          is_local: start.is_local || false
        )
      end

      private def self.proto_timestamp_to_time(ts : Google::Protobuf::Timestamp?) : Time?
        return nil if ts.nil?
        secs = ts.seconds || 0_i64
        nanos = ts.nanos || 0
        Time.unix(secs) + nanos.nanoseconds
      end

      private def self.proto_duration_to_span(d : Google::Protobuf::Duration?) : Time::Span?
        return nil if d.nil?
        Time::Span.new(seconds: d.seconds || 0_i64, nanoseconds: d.nanos || 0)
      end

      private def self.calculate_deadline(
        scheduled_time : Time,
        started_time : Time,
        start : Coresdk::ActivityTask::Start
      ) : Time
        candidates = [] of Time

        if s2c = start.schedule_to_close_timeout
          span = Time::Span.new(seconds: s2c.seconds || 0_i64, nanoseconds: s2c.nanos || 0)
          candidates << scheduled_time + span if span > Time::Span.zero
        end

        if stc = start.start_to_close_timeout
          span = Time::Span.new(seconds: stc.seconds || 0_i64, nanoseconds: stc.nanos || 0)
          candidates << started_time + span if span > Time::Span.zero
        end

        candidates.min? || Time.utc + 10.years
      end
    end
  end
end
