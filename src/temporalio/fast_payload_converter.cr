require "json"
require "./internal/proto"

module Temporalio
  # Fast-path payload converter that avoids JSON serialization for common types.
  # This is 5-10x faster than the standard PayloadConverter for primitives.
  #
  # Fast paths:
  # - Nil → no data, just encoding marker
  # - String → UTF-8 bytes directly, no JSON quotes
  # - Int64/Int32 → binary encoding (8/4 bytes)
  # - Bool → single byte (0 or 1)
  # - Float64 → binary encoding (8 bytes)
  #
  # Fallback to JSON for complex types (Arrays, Hashes, custom objects).
  module FastPayloadConverter
    extend self

    # Encoding markers
    NULL_ENCODING = "json/null"
    JSON_ENCODING = "json/plain"
    STRING_ENCODING = "binary/string"
    INT64_ENCODING = "binary/int64"
    INT32_ENCODING = "binary/int32"
    BOOL_ENCODING = "binary/bool"
    FLOAT64_ENCODING = "binary/float64"

    # ============================================================================
    # ENCODE (to_payload)
    # ============================================================================

    # Fast path: Nil → no data
    def to_payload(value : Nil) : Temporal::Api::Common::V1::Payload
      make_payload(NULL_ENCODING, nil)
    end

    # Fast path: String → direct UTF-8 bytes (no JSON encoding)
    def to_payload(value : String) : Temporal::Api::Common::V1::Payload
      make_payload(STRING_ENCODING, value.to_slice)
    end

    # Fast path: Int64 → 8 bytes little-endian
    def to_payload(value : Int64) : Temporal::Api::Common::V1::Payload
      bytes = Bytes.new(8)
      IO::ByteFormat::LittleEndian.encode(value, bytes)
      make_payload(INT64_ENCODING, bytes)
    end

    # Fast path: Int32 → 4 bytes little-endian
    def to_payload(value : Int32) : Temporal::Api::Common::V1::Payload
      bytes = Bytes.new(4)
      IO::ByteFormat::LittleEndian.encode(value, bytes)
      make_payload(INT32_ENCODING, bytes)
    end

    # Fast path: Bool → 1 byte (0 or 1)
    def to_payload(value : Bool) : Temporal::Api::Common::V1::Payload
      bytes = Bytes.new(1)
      bytes[0] = value ? 1_u8 : 0_u8
      make_payload(BOOL_ENCODING, bytes)
    end

    # Fast path: Float64 → 8 bytes IEEE 754
    def to_payload(value : Float64) : Temporal::Api::Common::V1::Payload
      bytes = Bytes.new(8)
      IO::ByteFormat::LittleEndian.encode(value.unsafe_as(Int64), bytes)
      make_payload(FLOAT64_ENCODING, bytes)
    end

    # Fallback: All other types → JSON encoding (same as standard converter)
    def to_payload(value : T) : Temporal::Api::Common::V1::Payload forall T
      make_payload(JSON_ENCODING, value.to_json.to_slice)
    end

    # ============================================================================
    # DECODE (from_payload)
    # ============================================================================

    # Decode to specific type
    def from_payload(payload : Temporal::Api::Common::V1::Payload, type : T.class) : T forall T
      enc = encoding_of(payload)
      data = payload.data

      # Handle null encoding
      if enc == NULL_ENCODING
        {% if T <= Nil %}
          return nil
        {% else %}
          raise Temporalio::Error.new("Cannot decode null payload as #{{{T}}}")
        {% end %}
      end

      raise Temporalio::Error.new("Payload has no data") if data.nil? || data.empty?

      # Fast paths for specific types
      {% if T == String %}
        case enc
        when STRING_ENCODING
          return String.new(data)
        when JSON_ENCODING
          # JSON-encoded string has quotes
          return JSON.parse(String.new(data)).as_s
        else
          raise Temporalio::Error.new("Cannot decode #{enc} as String")
        end
      {% elsif T == Int64 %}
        case enc
        when INT64_ENCODING
          return IO::ByteFormat::LittleEndian.decode(Int64, data)
        when JSON_ENCODING
          return JSON.parse(String.new(data)).as_i64
        else
          raise Temporalio::Error.new("Cannot decode #{enc} as Int64")
        end
      {% elsif T == Int32 %}
        case enc
        when INT32_ENCODING
          return IO::ByteFormat::LittleEndian.decode(Int32, data)
        when JSON_ENCODING
          return JSON.parse(String.new(data)).as_i
        else
          raise Temporalio::Error.new("Cannot decode #{enc} as Int32")
        end
      {% elsif T == Bool %}
        case enc
        when BOOL_ENCODING
          return data[0] != 0_u8
        when JSON_ENCODING
          return JSON.parse(String.new(data)).as_bool
        else
          raise Temporalio::Error.new("Cannot decode #{enc} as Bool")
        end
      {% elsif T == Float64 %}
        case enc
        when FLOAT64_ENCODING
          return IO::ByteFormat::LittleEndian.decode(Int64, data).unsafe_as(Float64)
        when JSON_ENCODING
          return JSON.parse(String.new(data)).as_f
        else
          raise Temporalio::Error.new("Cannot decode #{enc} as Float64")
        end
      {% elsif T <= Nil %}
        return nil
      {% else %}
        # Fallback to JSON for complex types
        if enc != JSON_ENCODING
          raise Temporalio::Error.new("Cannot decode #{enc} as #{{{T}}} (expected JSON)")
        end
        return T.from_json(String.new(data))
      {% end %}
    end

    # Decode to raw value (returns String or nil)
    def from_payload(payload : Temporal::Api::Common::V1::Payload) : String?
      enc = encoding_of(payload)
      data = payload.data

      case enc
      when NULL_ENCODING
        nil
      when STRING_ENCODING
        data ? String.new(data) : nil
      when JSON_ENCODING
        data ? String.new(data) : nil
      when INT64_ENCODING
        data ? IO::ByteFormat::LittleEndian.decode(Int64, data).to_s : nil
      when INT32_ENCODING
        data ? IO::ByteFormat::LittleEndian.decode(Int32, data).to_s : nil
      when BOOL_ENCODING
        data ? (data[0] != 0_u8).to_s : nil
      when FLOAT64_ENCODING
        data ? IO::ByteFormat::LittleEndian.decode(Int64, data).unsafe_as(Float64).to_s : nil
      else
        raise Temporalio::Error.new("Unknown payload encoding: #{enc.inspect}")
      end
    end

    # ============================================================================
    # HELPERS
    # ============================================================================

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

  # Fast DataConverter that uses FastPayloadConverter for primitives
  class FastDataConverter < DataConverter
    DEFAULT = new

    # Delegate to FastPayloadConverter
    def to_payload(value : Nil)
      FastPayloadConverter.to_payload(value)
    end

    def to_payload(value : String)
      FastPayloadConverter.to_payload(value)
    end

    def to_payload(value : Int64)
      FastPayloadConverter.to_payload(value)
    end

    def to_payload(value : Int32)
      FastPayloadConverter.to_payload(value)
    end

    def to_payload(value : Bool)
      FastPayloadConverter.to_payload(value)
    end

    def to_payload(value : Float64)
      FastPayloadConverter.to_payload(value)
    end

    def to_payload(value : T) forall T
      FastPayloadConverter.to_payload(value)
    end

    def from_payload(payload : Temporal::Api::Common::V1::Payload, type : T.class) : T forall T
      FastPayloadConverter.from_payload(payload, type)
    end

    def from_payload(payload : Temporal::Api::Common::V1::Payload) : String?
      FastPayloadConverter.from_payload(payload)
    end

    # Batch operations
    def to_payloads(payloads : Array(Temporal::Api::Common::V1::Payload)) : Array(Temporal::Api::Common::V1::Payload)
      payloads
    end

    def from_payloads(payloads : Array(Temporal::Api::Common::V1::Payload)) : Array(String?)
      payloads.map { |p| from_payload(p) }
    end

    def from_payloads_message(msg : Temporal::Api::Common::V1::Payloads?) : Array(String?)
      return [] of String? if msg.nil?
      payloads = msg.payloads
      return [] of String? if payloads.nil?
      from_payloads(payloads)
    end

    def to_payloads_message(payloads : Array(Temporal::Api::Common::V1::Payload)) : Temporal::Api::Common::V1::Payloads
      Temporal::Api::Common::V1::Payloads.new(payloads: payloads)
    end
  end
end
