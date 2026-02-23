require "spec"

# Test if FFI callbacks from external threads work in Crystal

lib LibTest
  fun create_thread(callback : (Void* -> Void), user_data : Void*) : Int32
end

describe "External Thread Callbacks" do
  it "can receive callback from pthread created thread" do
    channel = Channel(String).new(1)
    box = Box.new(channel)
    
    callback = ->(user_data : Void*) {
      STDERR.puts "DEBUG: Callback invoked from external thread!"
      STDERR.puts "DEBUG: Thread ID: #{Thread.current.inspect}"
      
      # Try to register this thread with GC
      {% if flag?(:preview_mt) %}
        GC.set_stackbottom(nil, Pointer(Void).null)
      {% else %}
        GC.set_stackbottom(Pointer(Void).null)
      {% end %}
      
      ch = Box(Channel(String)).unbox(user_data)
      ch.send("Hello from external thread!")
    }
    
    # This would need a simple C library that creates a pthread and calls the callback
    # For now, let's just test if we can compile with preview_mt
    pending "Need to create test C library" unless false
  end
end
