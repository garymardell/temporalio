require "./payload_converter"
require "./internal/proto"
require "./exceptions"

module Temporalio
  # DataConverter wraps PayloadConverter and provides batch encode/decode
  # as well as proto message helpers.
  #
  # All encoding is JSON. Binary encoding is not supported.
  #
  # Usage:
  #   dc = Temporalio::DataConverter::DEFAULT
  #
  #   # Encode — pass any value that responds to .to_json
  #   payload = dc.to_payload("hello")
  #   payload = dc.to_payload(42_i64)
  #   payload = dc.to_payload(my_serializable_object)
  #
  #   # Typed decode
  #   str  = dc.from_payload(payload, String)
  #   user = dc.from_payload(payload, MyUser)   # MyUser includes JSON::Serializable
  #
  #   # Untyped decode — returns raw JSON string
  #   json = dc.from_payload(payload)           # => String?
  class DataConverter
    DEFAULT = new

    # Encode nil explicitly → "json/null" payload.
    def to_payload(value : Nil) : Temporal::Api::Common::V1::Payload
      PayloadConverter.to_payload(value)
    end

    # Encode any value by calling .to_json.
    # Accepts String, Int*, Float*, Bool, Array, Hash, JSON::Any,
    # or any class/struct that includes JSON::Serializable.
    def to_payload(value : T) : Temporal::Api::Common::V1::Payload forall T
      PayloadConverter.to_payload(value)
    end

    # Decode a Payload to a specific type T using T.from_json (or JSON.parse for primitives).
    def from_payload(payload : Temporal::Api::Common::V1::Payload, type : T.class) : T forall T
      PayloadConverter.from_payload(payload, type)
    end

    # Decode a Payload to a raw JSON string. Returns nil for null payloads.
    def from_payload(payload : Temporal::Api::Common::V1::Payload) : String?
      PayloadConverter.from_payload(payload)
    end

    # Encode an array of already-encoded Payload objects (pass-through).
    def to_payloads(payloads : Array(Temporal::Api::Common::V1::Payload)) : Array(Temporal::Api::Common::V1::Payload)
      payloads
    end

    # Decode an array of Payloads to raw JSON strings.
    def from_payloads(payloads : Array(Temporal::Api::Common::V1::Payload)) : Array(String?)
      payloads.map { |p| from_payload(p) }
    end

    # Decode a Payloads proto message to raw JSON strings.
    def from_payloads_message(msg : Temporal::Api::Common::V1::Payloads?) : Array(String?)
      return [] of String? if msg.nil?
      payloads = msg.payloads
      return [] of String? if payloads.nil?
      from_payloads(payloads)
    end

    # Wrap an array of already-encoded Payloads in a Payloads proto message.
    def to_payloads_message(payloads : Array(Temporal::Api::Common::V1::Payload)) : Temporal::Api::Common::V1::Payloads
      Temporal::Api::Common::V1::Payloads.new(payloads: payloads)
    end
  end
end
