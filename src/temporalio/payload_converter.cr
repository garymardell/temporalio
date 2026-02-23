require "json"
require "./internal/proto"

module Temporalio
  # Encoding used for all JSON payloads.
  JSON_ENCODING = "json/plain"
  # Encoding used for null/nil payloads.
  NULL_ENCODING = "json/null"

  # Converts Crystal values to/from Temporal Payload using JSON encoding only.
  #
  # to_payload accepts any value and calls .to_json on it.
  # This includes primitives (String, Int, Float, Bool), arrays, hashes,
  # and any class/struct that includes JSON::Serializable.
  #
  # from_payload(payload, Type) decodes by calling Type.from_json on the payload data.
  # from_payload(payload) returns the raw JSON string.
  #
  # Binary encoding is intentionally not supported.
  module PayloadConverter
    extend self

    # Encode nil → "json/null" payload with no data.
    def to_payload(value : Nil) : Temporal::Api::Common::V1::Payload
      make_payload(NULL_ENCODING, nil)
    end

    # Encode any value by calling .to_json.
    # Works for String, Int*, Float*, Bool, Array, Hash, JSON::Any,
    # and any class/struct that includes JSON::Serializable.
    def to_payload(value : T) : Temporal::Api::Common::V1::Payload forall T
      make_payload(JSON_ENCODING, value.to_json.to_slice)
    end

    # Decode a Payload to a specific type T using T.from_json.
    # T can be any type that has a .from_json class method:
    #   - String, Int64, Int32, Float64, Bool via JSON::Any intermediate
    #   - Any class/struct including JSON::Serializable
    #
    # Returns nil if the payload has "json/null" encoding and T is Nil.
    # Raises for null payloads when T is not Nil.
    def from_payload(payload : Temporal::Api::Common::V1::Payload, type : T.class) : T forall T
      enc = encoding_of(payload)

      if enc == NULL_ENCODING
        {% if T <= Nil %}
          return nil
        {% else %}
          raise Temporalio::Error.new("Cannot decode null payload as #{{{T}}}")
        {% end %}
      end

      data = payload.data
      raise Temporalio::Error.new("Payload has no data") if data.nil? || data.empty?
      json_str = String.new(data)

      {% if T <= Nil %}
        nil
      {% elsif T == Bool %}
        JSON.parse(json_str).as_bool
      {% elsif T == Int64 %}
        JSON.parse(json_str).as_i64
      {% elsif T == Int32 %}
        JSON.parse(json_str).as_i
      {% elsif T == Float64 %}
        JSON.parse(json_str).as_f
      {% elsif T == Float32 %}
        JSON.parse(json_str).as_f32
      {% elsif T == String %}
        JSON.parse(json_str).as_s
      {% elsif T == JSON::Any %}
        JSON.parse(json_str)
      {% else %}
        T.from_json(json_str)
      {% end %}
    end

    # Decode a Payload to a raw JSON string. Returns nil for null payloads.
    def from_payload(payload : Temporal::Api::Common::V1::Payload) : String?
      enc = encoding_of(payload)
      case enc
      when NULL_ENCODING
        nil
      when JSON_ENCODING
        data = payload.data
        return nil if data.nil? || data.empty?
        String.new(data)
      else
        raise Temporalio::Error.new("Unknown payload encoding: #{enc.inspect}")
      end
    end

    private def make_payload(encoding : String, data : Bytes?) : Temporal::Api::Common::V1::Payload
      enc_entry = Temporal::Api::Common::V1::StringBytesEntry.new(
        key: "encoding",
        value: encoding.to_slice
      )
      Temporal::Api::Common::V1::Payload.new(
        metadata: [enc_entry],
        data: data
      )
    end

    private def encoding_of(payload : Temporal::Api::Common::V1::Payload) : String?
      payload.metadata.try(&.find { |e| e.key == "encoding" }).try { |e| String.new(e.value.not_nil!) }
    end
  end
end
