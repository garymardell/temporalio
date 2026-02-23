require "../spec_helper"

# Stress test: run many activities concurrently via ActivityEnvironment.

class StressActivity
  include Temporalio::Activity
  activity_name "StressActivity"

  def execute(n : Int64) : Int64
    n * 3_i64
  end
end

class SlowActivity
  include Temporalio::Activity
  activity_name "SlowActivity"

  def execute(millis : Int64) : String
    sleep(millis.milliseconds)
    "done after #{millis}ms"
  end
end

describe "Concurrent activity stress test" do
  it "handles 1000 sequential activity executions correctly" do
    dc = Temporalio::DataConverter::DEFAULT
    env = Temporalio::Testing::ActivityEnvironment.new(activity_type: "StressActivity")
    1000.times do |i|
      result = env.run(StressActivity.new, dc.to_payload(i.to_i64))
      dc.from_payload(result.not_nil!, Int64).should eq(i * 3)
    end
  end

  it "runs 200 activities concurrently with correct results" do
    dc = Temporalio::DataConverter::DEFAULT
    results = Channel(Tuple(Int32, Int64)).new(200)

    200.times do |i|
      spawn do
        env = Temporalio::Testing::ActivityEnvironment.new(activity_type: "StressActivity")
        result = env.run(StressActivity.new, dc.to_payload(i.to_i64))
        results.send({i, dc.from_payload(result.not_nil!, Int64)})
      end
    end

    collected = Hash(Int32, Int64).new
    200.times { i, r = results.receive; collected[i] = r }

    collected.size.should eq(200)
    collected.each do |i, result|
      result.should eq(i * 3)
    end
  end

  it "context is isolated per activity fiber" do
    dc = Temporalio::DataConverter::DEFAULT
    ch = Channel(String).new(2)

    spawn do
      env = Temporalio::Testing::ActivityEnvironment.new(
        activity_id: "activity-a",
        activity_type: "StressActivity"
      )
      env.run(StressActivity.new, dc.to_payload(1_i64))
      ctx = Temporalio::Activity::Context.current?
      ch.send(ctx ? "leaked" : "clean")
    end

    spawn do
      env = Temporalio::Testing::ActivityEnvironment.new(
        activity_id: "activity-b",
        activity_type: "StressActivity"
      )
      env.run(StressActivity.new, dc.to_payload(2_i64))
      ctx = Temporalio::Activity::Context.current?
      ch.send(ctx ? "leaked" : "clean")
    end

    r1 = ch.receive
    r2 = ch.receive
    r1.should eq("clean")
    r2.should eq("clean")
  end

  it "activity registry is cleaned up after uninstall" do
    dc = Temporalio::DataConverter::DEFAULT
    env = Temporalio::Testing::ActivityEnvironment.new
    env.run(StressActivity.new, dc.to_payload(5_i64))

    Temporalio::Activity::Context::REGISTRY_MUTEX.synchronize do
      Temporalio::Activity::Context::REGISTRY.size.should eq(0)
    end
  end
end
