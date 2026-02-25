require "./lib"
require "../exceptions"

module Temporalio
  module Ext
    # Initialize the Rust extension
    # This must be called before using any other extension functions
    def self.init
      result = LibTemporalioExt.temporalio_init
      raise Exception.new("Failed to initialize Temporalio extension") if result != 0
    end

    # Get the version of the Rust extension
    def self.version : String
      ptr = LibTemporalioExt.temporalio_version
      String.new(ptr)
    end

    # Wrapper for ByteArray that handles memory management
    class ByteArray
      def initialize(@array : LibTemporalioExt::ByteArray)
      end

      def to_bytes : Bytes
        if @array.data.null? || @array.len == 0
          Bytes.empty
        else
          # Copy the data - Crystal now owns it
          Bytes.new(@array.data, @array.len)
        end
      end

      def finalize
        LibTemporalioExt.temporalio_byte_array_free(@array)
      end
    end

    # Wrapper for TemporalioError - maps to RPCError for proper exception hierarchy
    class ExtError < Temporalio::RPCError
      def initialize(code : Int32, message : String)
        # Try to extract gRPC status from the error message
        # The Rust layer returns errors like "RPC failed: code: 'Some requested entity was not found'"
        grpc_status = extract_grpc_status(message) || 2 # Default to UNKNOWN
        super(message, grpc_status)
      end

      def self.from_c_error(error : LibTemporalioExt::TemporalioError) : self
        message = if error.message.data.null? || error.message.len == 0
                    "Unknown error"
                  else
                    String.new(error.message.data, error.message.len)
                  end

        result = new(error.code, message)
        LibTemporalioExt.temporalio_error_free(error)
        result
      end

      private def extract_grpc_status(message : String) : Int32?
        # Map common error messages to gRPC status codes
        return 5 if message.includes?("not found") || message.includes?("NOT_FOUND")
        return 6 if message.includes?("already exists") || message.includes?("ALREADY_EXISTS")
        return 3 if message.includes?("invalid") || message.includes?("INVALID_ARGUMENT")
        return 7 if message.includes?("permission") || message.includes?("PERMISSION_DENIED")
        return 14 if message.includes?("unavailable") || message.includes?("UNAVAILABLE")
        return 4 if message.includes?("deadline") || message.includes?("DEADLINE_EXCEEDED")
        nil
      end
    end

    # Wrapper for ClientHandle
    class Client
      def initialize(@handle : LibTemporalioExt::ClientHandle)
        raise Exception.new("Null client handle") if @handle.null?
      end

      def self.connect(target : String, namespace : String) : self
        # Ensure extension is initialized
        Ext.init

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        handle = LibTemporalioExt.temporalio_client_connect(
          target.to_unsafe, target.bytesize,
          namespace.to_unsafe, namespace.bytesize,
          pointerof(error)
        )

        if handle.null?
          raise ExtError.from_c_error(error)
        end

        new(handle)
      end

      def rpc_call(service : UInt32, rpc_name : String, request : Bytes) : Bytes
        response = uninitialized LibTemporalioExt::ByteArray
        response.data = Pointer(UInt8).null
        response.len = 0

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_client_rpc_call(
          @handle,
          service,
          rpc_name.to_unsafe, rpc_name.bytesize,
          request.to_unsafe, request.size,
          pointerof(response),
          pointerof(error)
        )

        if result != 0
          raise ExtError.from_c_error(error)
        end

        ByteArray.new(response).to_bytes
      end

      # Start an async (non-blocking) RPC call.
      # Returns an opaque handle. Poll with rpc_poll. Free with rpc_handle_free.
      def rpc_call_async(service : UInt32, rpc_name : String, request : Bytes) : LibTemporalioExt::AsyncRpcHandle
        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        handle = LibTemporalioExt.temporalio_client_rpc_call_async(
          @handle,
          service,
          rpc_name.to_unsafe, rpc_name.bytesize,
          request.to_unsafe, request.size,
          pointerof(error)
        )

        if handle.null?
          raise ExtError.from_c_error(error)
        end

        handle
      end

      # Poll an async RPC handle. Returns:
      #   {:done, bytes}  - completed successfully
      #   {:done, error}  - completed with error (raises)
      #   {:pending, nil} - still in progress
      def rpc_poll(async_handle : LibTemporalioExt::AsyncRpcHandle) : Bytes?
        response = uninitialized LibTemporalioExt::ByteArray
        response.data = Pointer(UInt8).null
        response.len = 0

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_client_rpc_poll(
          async_handle,
          pointerof(response),
          pointerof(error)
        )

        case result
        when 0
          # Done
          if error.code != 0
            raise ExtError.from_c_error(error)
          end
          ByteArray.new(response).to_bytes
        when 1
          # Still pending
          nil
        else
          raise ExtError.from_c_error(error)
        end
      end

      # Free an async RPC handle
      def rpc_handle_free(async_handle : LibTemporalioExt::AsyncRpcHandle) : Nil
        LibTemporalioExt.temporalio_client_rpc_handle_free(async_handle) unless async_handle.null?
      end

      def finalize
        LibTemporalioExt.temporalio_client_free(@handle) unless @handle.null?
      end
    end

    # Wrapper for WorkerHandle
    class Worker
      @handle : LibTemporalioExt::WorkerHandle
      @finalized : Bool = false

      def initialize(handle : LibTemporalioExt::WorkerHandle)
        raise Exception.new("Null worker handle") if handle.null?
        @handle = handle
      end

      def self.new(target : String, namespace : String, task_queue : String, max_cached_workflows : Int32 = 1000) : self
        # Ensure extension is initialized
        Ext.init

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        handle = LibTemporalioExt.temporalio_worker_new(
          target.to_unsafe, target.bytesize,
          namespace.to_unsafe, namespace.bytesize,
          task_queue.to_unsafe, task_queue.bytesize,
          max_cached_workflows.to_u64,
          pointerof(error)
        )

        if handle.null?
          raise ExtError.from_c_error(error)
        end

        new(handle)
      end

      def poll_workflow_activation : Bytes?
        # Poll the queue once (non-blocking)
        activation = uninitialized LibTemporalioExt::ByteArray
        activation.data = Pointer(UInt8).null
        activation.len = 0

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_worker_poll_workflow_activation(
          @handle,
          pointerof(activation),
          pointerof(error)
        )

        if result != 0
          raise ExtError.from_c_error(error)
        end

        # If we got data, return it; otherwise return nil
        if !activation.data.null? && activation.len > 0
          return ByteArray.new(activation).to_bytes
        end
        
        nil
      end

      def poll_activity_task : Bytes?
        # Poll the queue once (non-blocking)
        task = uninitialized LibTemporalioExt::ByteArray
        task.data = Pointer(UInt8).null
        task.len = 0

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_worker_poll_activity_task(
          @handle,
          pointerof(task),
          pointerof(error)
        )

        if result != 0
          raise ExtError.from_c_error(error)
        end

        # If we got data, return it; otherwise return nil
        if !task.data.null? && task.len > 0
          return ByteArray.new(task).to_bytes
        end
        
        nil
      end

      def complete_workflow_activation(completion : Bytes) : Nil
        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_worker_complete_workflow_activation(
          @handle,
          completion.to_unsafe, completion.size,
          pointerof(error)
        )

        if result != 0
          raise ExtError.from_c_error(error)
        end
      end

      def complete_activity_task(completion : Bytes) : Nil
        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_worker_complete_activity_task(
          @handle,
          completion.to_unsafe, completion.size,
          pointerof(error)
        )

        if result != 0
          raise ExtError.from_c_error(error)
        end
      end

      def initiate_shutdown : Nil
        LibTemporalioExt.temporalio_worker_initiate_shutdown(@handle)
      end

      def finalize_shutdown : Nil
        return if @finalized

        error = uninitialized LibTemporalioExt::TemporalioError
        error.code = 0
        error.message = LibTemporalioExt::ByteArray.new(data: Pointer(UInt8).null, len: 0)

        result = LibTemporalioExt.temporalio_worker_finalize_shutdown(
          @handle,
          pointerof(error)
        )

        if result != 0
          raise ExtError.from_c_error(error)
        end

        # finalize_shutdown consumes the handle
        @finalized = true
      end

      def finalize
        # Only free if not already finalized by finalize_shutdown
        LibTemporalioExt.temporalio_worker_free(@handle) unless @finalized || @handle.null?
      end
    end
  end
end
