require "./activity/context"
require "./activity/activity_info"
require "./data_converter"
require "./internal/proto"

module Temporalio
  # Mixin module for defining Temporal activities.
  #
  # Usage:
  #   class GreetActivity
  #     include Temporalio::Activity
  #
  #     activity_name "GreetActivity"  # optional — defaults to class name
  #
  #     def execute(name : String) : String
  #       "Hello, #{name}!"
  #     end
  #   end
  #
  # The `_temporal_execute` method is automatically generated when the class
  # definition completes. It introspects the `execute` method signature and
  # generates code to decode payloads into typed arguments and encode the result.
  #
  # `execute` arguments and return value must be JSON-serializable:
  # primitives (String, Int64, Float64, Bool), JSON::Any, Arrays, Hashes,
  # or any class/struct that includes JSON::Serializable.
  module Activity
    macro included
      # The Temporal activity type name. Defaults to the class name.
      def self.activity_name : String
        {{@type.name.stringify}}
      end

      # Returns the current activity context. Available inside any activity method.
      def activity : Temporalio::Activity::Context
        Temporalio::Activity::Context.current
      end

      # Override the registered activity name.
      macro activity_name(act_name)
        def self.activity_name : String
          \{{act_name}}
        end
      end

      # Automatically generate _temporal_execute when the class definition completes.
      # This introspects the execute method and generates code to decode each parameter
      # from payloads and encode the result.
      macro finished
        \{% method = @type.methods.find { |m| m.name == "execute" } %}
        \{% if method %}
          def _temporal_execute(
            payloads : Array(Temporal::Api::Common::V1::Payload),
            converter : Temporalio::DataConverter
          ) : Temporal::Api::Common::V1::Payload?
            \{% args = method.args %}
            \{% if args.size == 0 %}
              result = execute
            \{% else %}
              \{% for arg, idx in args %}
                _arg\{{idx}} = converter.from_payload(payloads[\{{idx}}], \{{arg.restriction}})
              \{% end %}
              result = execute(\{% for arg, idx in args %}_arg\{{idx}}\{% if idx < args.size - 1 %}, \{% end %}\{% end %})
            \{% end %}
            converter.to_payload(result)
          end
        \{% end %}
      end
    end
  end
end
