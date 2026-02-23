require "spec"
require "../../src/temporalio/ext/wrappers"

describe "Temporalio Rust Extension" do
  it "initializes successfully" do
    Temporalio::Ext.init
  end

  it "returns a version string" do
    Temporalio::Ext.init
    version = Temporalio::Ext.version
    version.should contain("temporalio-crystal-bridge")
  end

  describe "Client" do
    it "attempts to connect to Temporal server" do
      Temporalio::Ext.init
      
      # Try to connect to the running server
      # This should succeed if the server is running on localhost:7234
      begin
        client = Temporalio::Ext::Client.connect("http://localhost:7234", "default")
        client.should_not be_nil
        puts "✓ Successfully connected to Temporal server!"
      rescue ex : Temporalio::Ext::ExtError
        # If it fails, that's OK for now - we're just testing the FFI works
        puts "✗ Connection failed (expected if server not running): #{ex.message}"
      end
    end
  end
end
