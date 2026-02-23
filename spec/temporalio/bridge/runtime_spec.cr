require "../../spec_helper"

describe Temporalio::Bridge::Runtime do
  it "creates a runtime without raising" do
    runtime = Temporalio::Bridge::Runtime.new
    runtime.should_not be_nil
  end

  it "can create multiple independent runtimes sequentially" do
    r1 = Temporalio::Bridge::Runtime.new
    r2 = Temporalio::Bridge::Runtime.new
    r1.should_not be_nil
    r2.should_not be_nil
  end
end
