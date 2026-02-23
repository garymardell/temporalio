require "../ext/testing"
require "../client"
require "../worker"
require "../data_converter"
require "../internal/proto"

module Temporalio
  module Testing
    # Provides an embedded Temporal test server for workflow integration tests.
    #
    # Time-skipping mode (default):
    #   Temporalio::Testing::WorkflowEnvironment.start_time_skipping do |env|
    #     client = env.client
    #     # ... start workflows, assert results
    #   end
    #
    # Local dev server mode:
    #   Temporalio::Testing::WorkflowEnvironment.start_local do |env|
    #     client = env.client
    #   end
    class WorkflowEnvironment
      getter client : Client

      # Start an ephemeral test server with time-skipping enabled.
      def self.start_time_skipping(
        namespace : String = "default",
        data_converter : DataConverter = DataConverter::DEFAULT,
        &block : WorkflowEnvironment -> Nil
      ) : Nil
        env = start_ephemeral(namespace, data_converter, time_skipping: true)
        begin
          block.call(env)
        ensure
          env.shutdown
        end
      end

      # Start a local dev server (requires `temporal` CLI on PATH).
      def self.start_local(
        namespace : String = "default",
        data_converter : DataConverter = DataConverter::DEFAULT,
        &block : WorkflowEnvironment -> Nil
      ) : Nil
        env = start_ephemeral(namespace, data_converter, time_skipping: false)
        begin
          block.call(env)
        ensure
          env.shutdown
        end
      end

      # Advance server time by the given duration (only works in time-skipping mode).
      def sleep(duration : Time::Span) : Nil
        unlock_time_skipping
        begin
          secs = duration.total_seconds.to_i64
          nanos = duration.nanoseconds
          req = Temporal::Api::Testservice::V1::SleepRequest.new(
            duration: Google::Protobuf::Duration.new(seconds: secs, nanos: nanos)
          )
          @client.test_service_call("sleep", req.to_protobuf.to_slice)
        ensure
          lock_time_skipping
        end
      end

      # Return the current server time (only works in time-skipping mode).
      def now : Time
        req = Google::Protobuf::Empty.new
        bytes = @client.test_service_call("get_current_time", req.to_protobuf.to_slice)
        resp = Temporal::Api::Testservice::V1::GetCurrentTimeResponse.from_protobuf(IO::Memory.new(bytes))
        ts = resp.time
        return Time.utc unless ts
        secs = ts.seconds || 0_i64
        nanos = ts.nanos || 0
        Time.unix(secs) + nanos.nanoseconds
      end

      # Disable automatic time-skipping so time only advances on explicit sleep calls.
      def lock_time_skipping : Nil
        req = Google::Protobuf::Empty.new
        @client.test_service_call("lock_time_skipping", req.to_protobuf.to_slice)
      end

      # Re-enable automatic time-skipping.
      def unlock_time_skipping : Nil
        req = Google::Protobuf::Empty.new
        @client.test_service_call("unlock_time_skipping", req.to_protobuf.to_slice)
      end

      def shutdown : Nil
        return if @shutdown
        @shutdown = true
        @server.shutdown
      end

      protected def initialize(
        @client : Client,
        @server : Ext::Testing::EphemeralServer
      )
        @shutdown = false
      end

      private def self.start_ephemeral(
        namespace : String,
        data_converter : DataConverter,
        time_skipping : Bool
      ) : WorkflowEnvironment
        server = if time_skipping
          test_server = Ext::Testing::TestServer.new
          test_server.start
        else
          test_server = Ext::Testing::TestServer.new
          dev_server = Ext::Testing::DevServer.new(
            test_server: test_server,
            namespace: namespace
          )
          dev_server.start
        end

        client = Client.connect(server.target, namespace: namespace, data_converter: data_converter)
        env = allocate
        env.initialize(client, server)
        env
      end
    end
  end
end
