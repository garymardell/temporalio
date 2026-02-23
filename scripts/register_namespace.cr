#!/usr/bin/env crystal

require "../src/temporalio"

# Register the "default" namespace with the Temporal server
client = Temporalio::Client.connect(
  "http://localhost:7233",
  namespace: "temporal-system"  # System namespace should exist
)

puts "Connected to Temporal server"

# Create RegisterNamespaceRequest
req = Temporal::Api::Workflowservice::V1::RegisterNamespaceRequest.new(
  namespace: "default",
  workflow_execution_retention_period: Google::Protobuf::Duration.new(seconds: 86400_i64 * 7), # 7 days
  description: "Default namespace for testing"
)

puts "Registering namespace 'default'..."

begin
  resp = client.bridge_client.rpc_call(
    service: "workflow_service",
    rpc: "RegisterNamespace",
    req: req.to_protobuf.to_slice
  )
  puts "SUCCESS: Namespace 'default' registered"
rescue ex
  if ex.message.try &.includes?("already exists")
    puts "Namespace 'default' already exists"
  else
    puts "ERROR: #{ex.message}"
    exit 1
  end
end
