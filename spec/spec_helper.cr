require "spec"
require "../src/temporalio"

# Run a block with a timeout. Raises if the block does not complete within the given duration.
def with_timeout(duration : Time::Span, &block)
  done = Channel(Exception?).new(1)
  spawn do
    begin
      block.call
      done.send(nil)
    rescue ex
      done.send(ex)
    end
  end
  select
  when result = done.receive
    raise result if result
  when timeout(duration)
    raise "Test timed out after #{duration}"
  end
end
