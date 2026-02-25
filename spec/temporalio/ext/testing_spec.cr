require "../../spec_helper"
require "../../../src/temporalio/ext/testing"

describe Temporalio::Ext::Testing do
  describe "TestServer" do
    # Share a single server instance across tests to avoid repeated start/shutdown overhead
    server = Temporalio::Ext::Testing::TestServer.new.start

    it "can start a test server with a valid target" do
      server.target.should_not be_empty
      server.target.should contain("127.0.0.1")
    end

    it "target contains expected host format" do
      server.target.should match(/127\.0\.0\.1:\d+/)
    end

    server.shutdown
  end

  describe "DevServer" do
    # DevServer requires the Temporal CLI binary to be installed via download_dest_dir
    # These tests are pending until the binary is available in the test environment
    pending "can start and shutdown a dev server (requires Temporal CLI binary)"
    pending "can configure namespace (requires Temporal CLI binary)"
  end
end
