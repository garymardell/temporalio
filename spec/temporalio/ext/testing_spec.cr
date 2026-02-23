require "../../spec_helper"
require "../../../src/temporalio/ext/testing"

describe Temporalio::Ext::Testing do
  describe "TestServer" do
    it "can start and shutdown a test server" do
      server_config = Temporalio::Ext::Testing::TestServer.new
      server = server_config.start
      
      # Verify we got a valid target
      server.target.should_not be_empty
      server.target.should contain("127.0.0.1")
      
      # Shutdown the server
      server.shutdown
    end

    it "can configure port" do
      server_config = Temporalio::Ext::Testing::TestServer.new(port: 7233_u16)
      server = server_config.start
      
      server.target.should contain(":7233")
      
      server.shutdown
    end
  end

  describe "DevServer" do
    it "can start and shutdown a dev server" do
      server_config = Temporalio::Ext::Testing::DevServer.new
      server = server_config.start
      
      # Verify we got a valid target
      server.target.should_not be_empty
      server.target.should contain("127.0.0.1")
      
      # Shutdown the server
      server.shutdown
    end

    it "can configure namespace" do
      server_config = Temporalio::Ext::Testing::DevServer.new(namespace: "test-namespace")
      server = server_config.start
      
      server.target.should_not be_empty
      
      server.shutdown
    end
  end

  describe "EphemeralServer" do
    it "automatically shuts down on finalize" do
      server_config = Temporalio::Ext::Testing::TestServer.new
      server = server_config.start
      
      # Let the server go out of scope and be finalized
      # The finalizer should handle shutdown
      server = nil
      GC.collect
    end
  end
end
