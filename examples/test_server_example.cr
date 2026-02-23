require "../src/temporalio/ext/testing"
require "../src/temporalio/ext/lib"

# Initialize the Rust extension
LibTemporalioExt.temporalio_init

puts "Starting ephemeral test server..."

server_config = Temporalio::Ext::Testing::TestServer.new
server = server_config.start

puts "Server started successfully!"
puts "Target: #{server.target}"

puts "Shutting down server..."
server.shutdown

puts "Server shut down successfully!"
