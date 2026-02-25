require "../ext/wrappers"

module Temporalio
  module Bridge
    # Options for connecting to a Temporal server.
    class ClientOptions
      getter target_url : String
      getter namespace : String
      getter client_name : String
      getter client_version : String
      getter identity : String
      getter api_key : String?
      getter tls : ClientTlsOptions?
      getter retry_config : ClientRetryConfig?
      getter keep_alive : ClientKeepAliveConfig?
      getter metadata : Hash(String, String)

      def initialize(
        @target_url : String,
        @namespace : String,
        @client_name : String = "temporal-crystal",
        @client_version : String = Temporalio::VERSION,
        @identity : String = "#{Process.pid}@#{System.hostname}",
        @api_key : String? = nil,
        @tls : ClientTlsOptions? = nil,
        @retry_config : ClientRetryConfig? = nil,
        @keep_alive : ClientKeepAliveConfig? = nil,
        @metadata : Hash(String, String) = Hash(String, String).new
      )
      end
    end

    class ClientTlsOptions
      getter server_root_ca_cert : Bytes?
      getter domain : String?
      getter client_cert : Bytes?
      getter client_private_key : Bytes?

      def initialize(
        @server_root_ca_cert : Bytes? = nil,
        @domain : String? = nil,
        @client_cert : Bytes? = nil,
        @client_private_key : Bytes? = nil
      )
      end
    end

    class ClientRetryConfig
      # Placeholder for retry configuration
    end

    class ClientKeepAliveConfig
      # Placeholder for keep-alive configuration
    end

    # Bridge wrapper around the Rust extension client.
    # This provides backward compatibility with the old C bridge interface.
    class Client
      @client : Ext::Client
      @runtime : Runtime
      getter target_url : String
      getter namespace : String

      # Connects to a Temporal server using the new Rust extension.
      def self.connect(runtime : Runtime, options : ClientOptions) : Client
        # The runtime parameter is passed but Ext manages its own internal runtime
        client = Ext::Client.connect(options.target_url, options.namespace)
        new(client, runtime, options.target_url, options.namespace)
      end

      def initialize(@client : Ext::Client, @runtime : Runtime, @target_url : String, @namespace : String)
      end

      # Accessor for backward compatibility
      def runtime
        @runtime
      end

      # Make an RPC call to the Temporal server.
      # Service codes: 1 = Workflow, 2 = Operator, 3 = Cloud, 4 = Test, 5 = Health
      def rpc_call(service : Symbol | UInt32, rpc : String, request : Bytes) : Bytes
        service_code = resolve_service_code(service)
        @client.rpc_call(service_code, rpc, request)
      end

      # Start an async (non-blocking) RPC call. Returns immediately.
      # Returns a raw async handle pointer. Poll with rpc_poll_async. Free with rpc_free_async.
      def rpc_call_async(service : Symbol | UInt32, rpc : String, request : Bytes) : LibTemporalioExt::AsyncRpcHandle
        service_code = resolve_service_code(service)
        @client.rpc_call_async(service_code, rpc, request)
      end

      # Poll an async RPC handle. Returns response bytes when done, nil if still pending.
      # Raises on error.
      def rpc_poll_async(handle : LibTemporalioExt::AsyncRpcHandle) : Bytes?
        @client.rpc_poll(handle)
      end

      # Free an async RPC handle.
      def rpc_free_async(handle : LibTemporalioExt::AsyncRpcHandle) : Nil
        @client.rpc_handle_free(handle)
      end

      # Close the client connection
      # Required by DB::Pool interface
      def close
        # The Rust extension manages its own cleanup
        # This is a no-op for compatibility with connection pooling
      end

      private def resolve_service_code(service : Symbol | UInt32) : UInt32
        case service
        when Symbol
          case service
          when :workflow then 1_u32
          when :operator then 2_u32
          when :cloud then 3_u32
          when :test then 4_u32
          when :health then 5_u32
          else raise "Unknown service: #{service}"
          end
        when UInt32
          service.to_u32
        else
          raise "Invalid service type: #{service.class}"
        end
      end
    end

    # Runtime is no longer needed with the Rust extension, but we keep it for compatibility.
    class Runtime
      def self.new
        # Ensure the extension is initialized
        Ext.init
        new
      end

      def initialize
      end
    end
  end
end
