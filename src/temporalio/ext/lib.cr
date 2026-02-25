# Crystal FFI bindings for the Rust extension
@[Link(ldflags: "-L#{__DIR__}/../../../ext/crystal-bridge/target/release -ltemporalio_crystal")]
lib LibTemporalioExt
  # Initialization
  fun temporalio_init : Int32
  fun temporalio_version : UInt8*

  # Memory management
  struct ByteArray
    data : UInt8*
    len : LibC::SizeT
  end

  struct TemporalioError
    code : Int32
    message : ByteArray
  end

  fun temporalio_byte_array_free(array : ByteArray) : Void
  fun temporalio_error_free(error : TemporalioError) : Void

  # Client operations
  type ClientHandle = Void*

  fun temporalio_client_connect(
    target : UInt8*, target_len : LibC::SizeT,
    namespace : UInt8*, namespace_len : LibC::SizeT,
    out_error : TemporalioError*
  ) : ClientHandle

  fun temporalio_client_free(client : ClientHandle) : Void

  fun temporalio_client_rpc_call(
    client : ClientHandle,
    service : UInt32,
    rpc_name : UInt8*, rpc_len : LibC::SizeT,
    request : UInt8*, request_len : LibC::SizeT,
    out_response : ByteArray*,
    out_error : TemporalioError*
  ) : Int32

  # Async (non-blocking) RPC call - returns immediately with a handle to poll
  type AsyncRpcHandle = Void*

  fun temporalio_client_rpc_call_async(
    client : ClientHandle,
    service : UInt32,
    rpc_name : UInt8*, rpc_len : LibC::SizeT,
    request : UInt8*, request_len : LibC::SizeT,
    out_error : TemporalioError*
  ) : AsyncRpcHandle

  # Poll async RPC handle. Returns: 0 = done, 1 = pending, -1 = error
  fun temporalio_client_rpc_poll(
    handle : AsyncRpcHandle,
    out_response : ByteArray*,
    out_error : TemporalioError*
  ) : Int32

  fun temporalio_client_rpc_handle_free(handle : AsyncRpcHandle) : Void

  # Worker operations
  type WorkerHandle = Void*

  fun temporalio_worker_new(
    target : UInt8*, target_len : LibC::SizeT,
    namespace : UInt8*, namespace_len : LibC::SizeT,
    task_queue : UInt8*, task_queue_len : LibC::SizeT,
    max_cached_workflows : LibC::SizeT,
    out_error : TemporalioError*
  ) : WorkerHandle

  fun temporalio_worker_poll_workflow_activation(
    worker : WorkerHandle,
    out_activation : ByteArray*,
    out_error : TemporalioError*
  ) : Int32

  fun temporalio_worker_poll_activity_task(
    worker : WorkerHandle,
    out_task : ByteArray*,
    out_error : TemporalioError*
  ) : Int32

  fun temporalio_worker_complete_workflow_activation(
    worker : WorkerHandle,
    completion : UInt8*, completion_len : LibC::SizeT,
    out_error : TemporalioError*
  ) : Int32

  fun temporalio_worker_complete_activity_task(
    worker : WorkerHandle,
    completion : UInt8*, completion_len : LibC::SizeT,
    out_error : TemporalioError*
  ) : Int32

  fun temporalio_worker_initiate_shutdown(
    worker : WorkerHandle
  ) : Void

  fun temporalio_worker_finalize_shutdown(
    worker : WorkerHandle,
    out_error : TemporalioError*
  ) : Int32

  fun temporalio_worker_free(worker : WorkerHandle) : Void
end
