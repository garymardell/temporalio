require "protobuf"

# Google Well-Known Types — not bundled with protobuf.cr, defined here.
# Required by any proto file that references google.protobuf.Duration/Timestamp/Empty.

module Google
  module Protobuf
    struct Duration
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seconds, :int64, 1
        optional :nanos, :int32, 2
      end
    end

    struct Timestamp
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :seconds, :int64, 1
        optional :nanos, :int32, 2
      end
    end

    struct Empty
      include ::Protobuf::Message
      contract_of "proto3" do
      end
    end
  end
end
