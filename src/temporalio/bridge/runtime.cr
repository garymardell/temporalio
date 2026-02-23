module Temporalio
  module Bridge
    # Runtime compatibility wrapper.
    # The Rust extension manages its own internal Tokio runtime.
    # This class is kept for backward compatibility with existing code.
    class Runtime
      def self.new
        # Ensure the Rust extension is initialized
        Ext.init
        new
      end

      def initialize
        # The Ext module handles runtime initialization internally
      end

      def to_unsafe
        # Not needed - Ext manages runtime internally
        raise "Runtime#to_unsafe is not supported with Rust extension"
      end

      def free_byte_array(arr)
        # Not needed - Ext manages memory internally
        # This method is kept for compatibility but does nothing
      end
    end
  end
end
