require "../spec_helper"
require "../support/activities/hello_activity"
require "../support/activities/heartbeat_activity"
require "../support/activities/failing_activity"

describe Temporalio::Activity do
  describe "activity_name" do
    it "defaults to class name" do
      HelloActivity.activity_name.should eq("HelloActivity")
    end

    it "can be overridden with activity_name macro" do
      HelloActivity.activity_name.should eq("HelloActivity")
    end
  end

  describe "_temporal_execute" do
    it "calls execute with decoded args via temporal_dispatch and returns encoded result" do
      dc = Temporalio::DataConverter::DEFAULT
      instance = HelloActivity.new

      payload = dc.to_payload("World")
      result = instance._temporal_execute([payload], dc)
      result.should_not be_nil
      dc.from_payload(result.not_nil!, String).should eq("Hello, World!")
    end
  end
end

describe Temporalio::Testing::ActivityEnvironment do
  it "runs a simple activity and returns the result" do
    dc = Temporalio::DataConverter::DEFAULT
    env = Temporalio::Testing::ActivityEnvironment.new
    result = env.run(HelloActivity.new, dc.to_payload("Crystal"))
    result.should_not be_nil
    dc.from_payload(result.not_nil!, String).should eq("Hello, Crystal!")
  end

  it "captures heartbeats" do
    dc = Temporalio::DataConverter::DEFAULT
    beats = [] of Array(Temporal::Api::Common::V1::Payload)
    env = Temporalio::Testing::ActivityEnvironment.new(
      on_heartbeat: ->(details : Array(Temporal::Api::Common::V1::Payload)) { beats << details }
    )
    env.run(HeartbeatActivity.new, dc.to_payload(3_i64))
    beats.size.should eq(3)
    dc.from_payload(beats[0][0], Int64).should eq(0_i64)
    dc.from_payload(beats[1][0], Int64).should eq(1_i64)
    dc.from_payload(beats[2][0], Int64).should eq(2_i64)
  end

  it "propagates ApplicationError from activity" do
    dc = Temporalio::DataConverter::DEFAULT
    env = Temporalio::Testing::ActivityEnvironment.new
    expect_raises(Temporalio::ApplicationError, "boom") do
      env.run(FailingActivity.new, dc.to_payload("boom"))
    end
  end

  it "raises CancelledError when activity checks cancellation after cancel!" do
    dc = Temporalio::DataConverter::DEFAULT
    env = Temporalio::Testing::ActivityEnvironment.new
    env.cancel!

    expect_raises(Temporalio::CancelledError) do
      env.run(HeartbeatActivity.new, dc.to_payload(5_i64))
    end
  end

  it "provides context info with correct task queue" do
    env = Temporalio::Testing::ActivityEnvironment.new(
      task_queue: "custom-queue",
      attempt: 3
    )
    env.info.task_queue.should eq("custom-queue")
    env.info.attempt.should eq(3)
  end
end

describe Temporalio::Activity::Context do
  it "raises when accessed outside activity fiber" do
    expect_raises(Temporalio::Error) do
      Temporalio::Activity::Context.current
    end
  end
end
