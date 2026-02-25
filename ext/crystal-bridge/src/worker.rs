use crossbeam_channel::{unbounded, Receiver, TryRecvError};
use prost::Message;
use std::os::raw::c_int;
use std::slice;
use std::sync::Arc;
use std::thread;
use temporalio_client::ClientOptionsBuilder;
use temporalio_common::{
    protos::coresdk::{workflow_completion::WorkflowActivationCompletion, ActivityTaskCompletion},
    worker::{WorkerConfigBuilder, WorkerVersioningStrategy},
    Worker as WorkerTrait,
};
use temporalio_sdk_core::{init_worker, Worker as CoreWorker};
use url::Url;

use crate::memory::{ByteArray, TemporalioError};
use crate::runtime::{get_core_runtime, get_runtime, init_core_runtime};

/// Poll result from background thread
enum PollResult {
    WorkflowActivation(Vec<u8>),
    ActivityTask(Vec<u8>),
    Shutdown,
}

/// Opaque handle for the Temporal worker
pub struct WorkerHandle {
    worker: Arc<CoreWorker>,
    workflow_rx: Receiver<PollResult>,
    activity_rx: Receiver<PollResult>,
}

/// Create a new worker
#[no_mangle]
pub extern "C" fn temporalio_worker_new(
    target: *const u8,
    target_len: usize,
    namespace: *const u8,
    namespace_len: usize,
    task_queue: *const u8,
    task_queue_len: usize,
    max_cached_workflows: usize,
    out_error: *mut TemporalioError,
) -> *mut WorkerHandle {
    let target_str = unsafe {
        let slice = slice::from_raw_parts(target, target_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                *out_error = TemporalioError::new(-1, format!("Invalid target: {}", e));
                return std::ptr::null_mut();
            }
        }
    };

    let namespace_str = unsafe {
        let slice = slice::from_raw_parts(namespace, namespace_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                *out_error = TemporalioError::new(-1, format!("Invalid namespace: {}", e));
                return std::ptr::null_mut();
            }
        }
    };

    let task_queue_str = unsafe {
        let slice = slice::from_raw_parts(task_queue, task_queue_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                *out_error = TemporalioError::new(-1, format!("Invalid task_queue: {}", e));
                return std::ptr::null_mut();
            }
        }
    };

    // Ensure CoreRuntime is initialized
    if let Err(e) = init_core_runtime() {
        unsafe {
            *out_error = TemporalioError::new(-1, format!("Failed to init CoreRuntime: {}", e));
        }
        return std::ptr::null_mut();
    }

    let core_runtime = match get_core_runtime() {
        Ok(rt) => rt,
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, format!("Failed to get CoreRuntime: {}", e));
            }
            return std::ptr::null_mut();
        }
    };

    // Parse target URL
    let target_url = match Url::parse(target_str) {
        Ok(url) => url,
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, format!("Invalid target URL: {}", e));
            }
            return std::ptr::null_mut();
        }
    };

    // Build client options
    let client_opts = match ClientOptionsBuilder::default()
        .target_url(target_url)
        .client_name("temporal-crystal")
        .client_version(env!("CARGO_PKG_VERSION"))
        .identity(format!("{}@temporal-crystal", std::process::id()))
        .build()
    {
        Ok(opts) => opts,
        Err(e) => {
            unsafe {
                *out_error =
                    TemporalioError::new(-1, format!("Failed to build client options: {}", e));
            }
            return std::ptr::null_mut();
        }
    };

    // Build worker config
    let worker_config = match WorkerConfigBuilder::default()
        .namespace(namespace_str)
        .task_queue(task_queue_str)
        .max_cached_workflows(max_cached_workflows)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: String::new(),
        })
        .build()
    {
        Ok(cfg) => cfg,
        Err(e) => {
            unsafe {
                *out_error =
                    TemporalioError::new(-1, format!("Failed to build worker config: {}", e));
            }
            return std::ptr::null_mut();
        }
    };

    let runtime = get_runtime();

    let worker_result = runtime.block_on(async {
        let client = client_opts
            .connect(
                namespace_str,
                core_runtime.telemetry().get_temporal_metric_meter(),
            )
            .await
            .map_err(|e| format!("Client connection failed: {}", e))?;

        let worker = init_worker(core_runtime, worker_config, client.into_inner())
            .map_err(|e| format!("Worker creation failed: {}", e))?;

        Ok(worker)
    });

    match worker_result {
        Ok(worker) => {
            let worker_arc = Arc::new(worker);

            // Create crossbeam channels for workflow and activity results
            let (workflow_tx, workflow_rx) = unbounded();
            let (activity_tx, activity_rx) = unbounded();

            // Spawn background OS thread for workflow polling
            let worker_for_workflow = worker_arc.clone();
            let workflow_tx_clone = workflow_tx.clone();
            let runtime_clone = get_runtime();
            thread::spawn(move || {
                loop {
                    let result = runtime_clone
                        .block_on(async { worker_for_workflow.poll_workflow_activation().await });

                    match result {
                        Ok(activation) => {
                            let bytes = activation.encode_to_vec();
                            if bytes.is_empty() {
                                continue;
                            }
                            if let Err(_) =
                                workflow_tx_clone.send(PollResult::WorkflowActivation(bytes))
                            {
                                break;
                            }
                        }
                        Err(_) => {
                            let _ = workflow_tx_clone.send(PollResult::Shutdown);
                            break;
                        }
                    }
                }
            });

            // Spawn background OS thread for activity polling
            let worker_for_activity = worker_arc.clone();
            let activity_tx_clone = activity_tx.clone();
            let runtime_clone2 = get_runtime();
            thread::spawn(move || {
                loop {
                    let result = runtime_clone2
                        .block_on(async { worker_for_activity.poll_activity_task().await });

                    match result {
                        Ok(task) => {
                            let bytes = task.encode_to_vec();
                            if bytes.is_empty() {
                                continue;
                            }
                            if let Err(_) = activity_tx_clone.send(PollResult::ActivityTask(bytes))
                            {
                                break;
                            }
                        }
                        Err(_) => {
                            let _ = activity_tx_clone.send(PollResult::Shutdown);
                            break;
                        }
                    }
                }
            });

            let handle = WorkerHandle {
                worker: worker_arc,
                workflow_rx,
                activity_rx,
            };
            Box::into_raw(Box::new(handle))
        }
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, e);
            }
            std::ptr::null_mut()
        }
    }
}

/// Poll for a workflow activation - non-blocking, returns immediately
#[no_mangle]
pub extern "C" fn temporalio_worker_poll_workflow_activation(
    worker: *mut WorkerHandle,
    out_activation: *mut ByteArray,
    out_error: *mut TemporalioError,
) -> c_int {
    let worker_handle = unsafe {
        match worker.as_ref() {
            Some(w) => w,
            None => {
                *out_error = TemporalioError::new(-1, "Null worker handle".to_string());
                return -1;
            }
        }
    };

    // Try to receive from channel (non-blocking)
    let result = worker_handle.workflow_rx.try_recv();

    match result {
        Ok(PollResult::WorkflowActivation(bytes)) => {
            unsafe {
                *out_activation = ByteArray::from_vec(bytes);
            }
            0
        }
        Ok(PollResult::Shutdown) => {
            unsafe {
                *out_activation = ByteArray::empty();
            }
            0
        }
        Err(TryRecvError::Empty) => {
            unsafe {
                *out_activation = ByteArray::empty();
            }
            0
        }
        Err(TryRecvError::Disconnected) => {
            unsafe {
                *out_activation = ByteArray::empty();
            }
            0
        }
        _ => {
            unsafe {
                *out_activation = ByteArray::empty();
            }
            0
        }
    }
}

/// Poll for an activity task - non-blocking, returns immediately
#[no_mangle]
pub extern "C" fn temporalio_worker_poll_activity_task(
    worker: *mut WorkerHandle,
    out_task: *mut ByteArray,
    out_error: *mut TemporalioError,
) -> c_int {
    let worker_handle = unsafe {
        match worker.as_ref() {
            Some(w) => w,
            None => {
                *out_error = TemporalioError::new(-1, "Null worker handle".to_string());
                return -1;
            }
        }
    };

    // Try to receive from channel (non-blocking)
    match worker_handle.activity_rx.try_recv() {
        Ok(PollResult::ActivityTask(bytes)) => {
            unsafe {
                *out_task = ByteArray::from_vec(bytes);
            }
            0
        }
        Ok(PollResult::Shutdown) => {
            unsafe {
                *out_task = ByteArray::empty();
            }
            0
        }
        Err(TryRecvError::Empty) => {
            unsafe {
                *out_task = ByteArray::empty();
            }
            0
        }
        Err(TryRecvError::Disconnected) => {
            unsafe {
                *out_task = ByteArray::empty();
            }
            0
        }
        _ => {
            unsafe {
                *out_task = ByteArray::empty();
            }
            0
        }
    }
}

/// Complete a workflow activation
#[no_mangle]
pub extern "C" fn temporalio_worker_complete_workflow_activation(
    worker: *mut WorkerHandle,
    completion: *const u8,
    completion_len: usize,
    out_error: *mut TemporalioError,
) -> c_int {
    let worker_handle = unsafe {
        match worker.as_ref() {
            Some(w) => w,
            None => {
                *out_error = TemporalioError::new(-1, "Null worker handle".to_string());
                return -1;
            }
        }
    };

    let completion_bytes = unsafe { slice::from_raw_parts(completion, completion_len) };

    let completion_msg = match WorkflowActivationCompletion::decode(completion_bytes) {
        Ok(c) => c,
        Err(e) => {
            unsafe {
                *out_error =
                    TemporalioError::new(-1, format!("Failed to decode completion: {}", e));
            }
            return -1;
        }
    };

    let runtime = get_runtime();

    let result = runtime.block_on(async {
        worker_handle
            .worker
            .complete_workflow_activation(completion_msg)
            .await
    });

    match result {
        Ok(_) => 0,
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, format!("Complete failed: {}", e));
            }
            -1
        }
    }
}

/// Complete an activity task
#[no_mangle]
pub extern "C" fn temporalio_worker_complete_activity_task(
    worker: *mut WorkerHandle,
    completion: *const u8,
    completion_len: usize,
    out_error: *mut TemporalioError,
) -> c_int {
    let worker_handle = unsafe {
        match worker.as_ref() {
            Some(w) => w,
            None => {
                *out_error = TemporalioError::new(-1, "Null worker handle".to_string());
                return -1;
            }
        }
    };

    let completion_bytes = unsafe { slice::from_raw_parts(completion, completion_len) };

    let completion_msg = match ActivityTaskCompletion::decode(completion_bytes) {
        Ok(c) => c,
        Err(e) => {
            unsafe {
                *out_error =
                    TemporalioError::new(-1, format!("Failed to decode completion: {}", e));
            }
            return -1;
        }
    };

    let runtime = get_runtime();

    let result = runtime.block_on(async {
        worker_handle
            .worker
            .complete_activity_task(completion_msg)
            .await
    });

    match result {
        Ok(_) => 0,
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, format!("Complete failed: {}", e));
            }
            -1
        }
    }
}

/// Free a worker handle
#[no_mangle]
pub extern "C" fn temporalio_worker_free(worker: *mut WorkerHandle) {
    if !worker.is_null() {
        unsafe {
            let _ = Box::from_raw(worker);
        }
    }
}

/// Initiate worker shutdown
#[no_mangle]
pub extern "C" fn temporalio_worker_initiate_shutdown(worker: *mut WorkerHandle) {
    if let Some(worker_handle) = unsafe { worker.as_ref() } {
        worker_handle.worker.initiate_shutdown();
    }
}

/// Finalize worker shutdown - this consumes the worker
#[no_mangle]
pub extern "C" fn temporalio_worker_finalize_shutdown(
    worker: *mut WorkerHandle,
    _out_error: *mut TemporalioError,
) -> c_int {
    if worker.is_null() {
        return -1;
    }

    // Take ownership of the worker
    let worker_handle = unsafe { Box::from_raw(worker) };

    // Try to extract the worker from Arc
    let worker = match Arc::try_unwrap(worker_handle.worker) {
        Ok(w) => w,
        Err(_) => {
            // Still has other references, just return
            return -1;
        }
    };

    let runtime = get_runtime();

    runtime.block_on(async { worker.finalize_shutdown().await });

    0
}
