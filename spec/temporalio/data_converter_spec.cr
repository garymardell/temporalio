require "../spec_helper"
require "../../src/temporalio/data_converter"

struct SpecPoint
  include JSON::Serializable
  getter x : Int32
  getter y : Int32
  def initialize(@x : Int32, @y : Int32); end
end

describe Temporalio::DataConverter do
  dc = Temporalio::DataConverter::DEFAULT

  describe "#to_payload / #from_payload round-trip" do
    it "round-trips nil" do
      p = dc.to_payload(nil)
      enc = String.new(p.metadata.not_nil!.find! { |e| e.key == "encoding" }.value.not_nil!)
      enc.should eq("json/null")
      p.data.should be_nil
      dc.from_payload(p).should be_nil
    end

    it "round-trips String via JSON" do
      p = dc.to_payload("hello")
      enc = String.new(p.metadata.not_nil!.find! { |e| e.key == "encoding" }.value.not_nil!)
      enc.should eq("json/plain")
      dc.from_payload(p, String).should eq("hello")
    end

    it "round-trips Int64 via JSON" do
      p = dc.to_payload(42_i64)
      dc.from_payload(p, Int64).should eq(42_i64)
    end

    it "round-trips Float64 via JSON" do
      p = dc.to_payload(3.14_f64)
      dc.from_payload(p, Float64).should be_close(3.14, 0.001)
    end

    it "round-trips Bool via JSON" do
      p = dc.to_payload(true)
      dc.from_payload(p, Bool).should be_true
    end

    it "round-trips JSON::Any object" do
      obj = JSON.parse(%({"key":"value","num":1}))
      p = dc.to_payload(obj)
      result = dc.from_payload(p, JSON::Any)
      result["key"].as_s.should eq("value")
      result["num"].as_i.should eq(1)
    end

    it "returns raw JSON string for untyped from_payload" do
      p = dc.to_payload("hello")
      raw = dc.from_payload(p)
      raw.should_not be_nil
      raw.not_nil!.should eq(%("hello"))
    end
  end

  describe "#to_payloads / #from_payloads" do
    it "handles an array of payloads" do
      payloads = [dc.to_payload(nil), dc.to_payload("hello"), dc.to_payload(42_i64)]
      payloads.size.should eq(3)

      restored = dc.from_payloads(payloads)
      restored[0].should be_nil
      restored[1].not_nil!.should eq(%("hello"))
      restored[2].not_nil!.should eq("42")
    end
  end

  describe "#from_payloads_message" do
    it "returns empty array for nil message" do
      dc.from_payloads_message(nil).should eq([] of String?)
    end

    it "decodes a Payloads proto message to raw JSON strings" do
      msg = Temporal::Api::Common::V1::Payloads.new(
        payloads: [dc.to_payload("test")]
      )
      result = dc.from_payloads_message(msg)
      result.size.should eq(1)
      result[0].should eq(%("test"))
    end
  end

  describe "#to_payloads_message" do
    it "wraps payloads in a Payloads message" do
      payloads = [dc.to_payload("hello"), dc.to_payload(nil)]
      msg = dc.to_payloads_message(payloads)
      msg.payloads.not_nil!.size.should eq(2)
    end
  end

  describe "unknown encoding" do
    it "raises for unknown encoding" do
      bad_enc = Temporal::Api::Common::V1::StringBytesEntry.new(
        key: "encoding",
        value: "unknown/custom".to_slice
      )
      bad_payload = Temporal::Api::Common::V1::Payload.new(
        metadata: [bad_enc],
        data: "data".to_slice
      )
      expect_raises(Temporalio::Error, /Unknown payload encoding/) do
        dc.from_payload(bad_payload)
      end
    end
  end

  describe "JSON::Serializable support" do
    it "round-trips a JSON::Serializable object" do
      pt = SpecPoint.new(3, 7)
      p = dc.to_payload(pt)
      enc = String.new(p.metadata.not_nil!.find! { |e| e.key == "encoding" }.value.not_nil!)
      enc.should eq("json/plain")
      decoded = dc.from_payload(p, SpecPoint)
      decoded.x.should eq(3)
      decoded.y.should eq(7)
    end
  end
end
