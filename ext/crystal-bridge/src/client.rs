use prost::Message;
use std::os::raw::c_int;
use std::slice;
use std::sync::Arc;
use temporalio_client::{Client, ClientOptionsBuilder, RetryClient, TestService, WorkflowService};
use temporalio_common::protos::temporal::api::workflowservice::v1::*;
use tonic::IntoRequest;
use url::Url;

use crate::memory::{ByteArray, TemporalioError};
use crate::runtime::get_runtime;

/// Opaque handle for the Temporal client
pub struct ClientHandle {
    pub(crate) client: Arc<RetryClient<Client>>,
    pub(crate) namespace: String,
    pub(crate) target_url: String,
}

/// Connect to a Temporal server
/// Returns a ClientHandle on success, or writes error information to out_error
#[no_mangle]
pub extern "C" fn temporalio_client_connect(
    target: *const u8,
    target_len: usize,
    namespace: *const u8,
    namespace_len: usize,
    out_error: *mut TemporalioError,
) -> *mut ClientHandle {
    // Convert C strings to Rust strings
    let target_str = unsafe {
        let slice = slice::from_raw_parts(target, target_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                *out_error = TemporalioError::new(-1, format!("Invalid target URL: {}", e));
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

    // Get runtime
    let runtime = get_runtime();

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

    // Connect using blocking pattern
    let client_result = runtime.block_on(async { client_opts.connect(namespace_str, None).await });

    match client_result {
        Ok(client) => {
            let handle = ClientHandle {
                client: Arc::new(client),
                namespace: namespace_str.to_string(),
                target_url: target_str.to_string(),
            };
            Box::into_raw(Box::new(handle))
        }
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, format!("Connection failed: {}", e));
            }
            std::ptr::null_mut()
        }
    }
}

/// Free a client handle
#[no_mangle]
pub extern "C" fn temporalio_client_free(client: *mut ClientHandle) {
    if !client.is_null() {
        unsafe {
            let _ = Box::from_raw(client);
        }
    }
}

/// Make an RPC call to the Temporal server
#[no_mangle]
pub extern "C" fn temporalio_client_rpc_call(
    client: *mut ClientHandle,
    service: u32,
    rpc_name: *const u8,
    rpc_len: usize,
    request: *const u8,
    request_len: usize,
    out_response: *mut ByteArray,
    out_error: *mut TemporalioError,
) -> c_int {
    let client_handle = unsafe {
        match client.as_ref() {
            Some(c) => c,
            None => {
                *out_error = TemporalioError::new(-1, "Null client handle".to_string());
                return -1;
            }
        }
    };

    let rpc_str = unsafe {
        let slice = slice::from_raw_parts(rpc_name, rpc_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                *out_error = TemporalioError::new(-1, format!("Invalid RPC name: {}", e));
                return -1;
            }
        }
    };

    let request_bytes = unsafe { slice::from_raw_parts(request, request_len) };

    // Get runtime
    let runtime = get_runtime();

    // Clone the client (RetryClient is cheaply cloneable)
    let mut client = (*client_handle.client).clone();

    // Dispatch RPC based on service and name
    let result = runtime
        .block_on(async { dispatch_rpc(&mut client, service, rpc_str, request_bytes).await });

    match result {
        Ok(response_bytes) => {
            unsafe {
                *out_response = ByteArray::from_vec(response_bytes);
            }
            0
        }
        Err(e) => {
            unsafe {
                *out_error = TemporalioError::new(-1, format!("RPC call failed: {}", e));
            }
            -1
        }
    }
}

/// Macro to handle RPC calls with consistent decode/call/encode pattern
macro_rules! rpc_call {
    ($client:expr, $method:ident, $request_type:ty, $request_bytes:expr) => {{
        let req = <$request_type>::decode($request_bytes)
            .map_err(|e| format!("Failed to decode request: {}", e))?;
        let resp = $client
            .$method(req.into_request())
            .await
            .map_err(|e| format!("RPC failed: {}", e))?;
        let mut buf = Vec::new();
        resp.into_inner()
            .encode(&mut buf)
            .map_err(|e| format!("Failed to encode response: {}", e))?;
        Ok(buf)
    }};
}

/// Dispatch RPC calls to the appropriate service method
async fn dispatch_rpc(
    client: &mut RetryClient<Client>,
    service: u32,
    rpc_name: &str,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    match service {
        1 => {
            // Workflow service
            match rpc_name {
                "StartWorkflowExecution" => {
                    rpc_call!(
                        client,
                        start_workflow_execution,
                        StartWorkflowExecutionRequest,
                        request_bytes
                    )
                }
                "GetWorkflowExecutionHistory" => {
                    rpc_call!(
                        client,
                        get_workflow_execution_history,
                        GetWorkflowExecutionHistoryRequest,
                        request_bytes
                    )
                }
                "SignalWorkflowExecution" => {
                    rpc_call!(
                        client,
                        signal_workflow_execution,
                        SignalWorkflowExecutionRequest,
                        request_bytes
                    )
                }
                "QueryWorkflow" => {
                    rpc_call!(client, query_workflow, QueryWorkflowRequest, request_bytes)
                }
                "DescribeWorkflowExecution" => {
                    rpc_call!(
                        client,
                        describe_workflow_execution,
                        DescribeWorkflowExecutionRequest,
                        request_bytes
                    )
                }
                "RequestCancelWorkflowExecution" => {
                    rpc_call!(
                        client,
                        request_cancel_workflow_execution,
                        RequestCancelWorkflowExecutionRequest,
                        request_bytes
                    )
                }
                "TerminateWorkflowExecution" => {
                    rpc_call!(
                        client,
                        terminate_workflow_execution,
                        TerminateWorkflowExecutionRequest,
                        request_bytes
                    )
                }
                "ListOpenWorkflowExecutions" => {
                    rpc_call!(
                        client,
                        list_open_workflow_executions,
                        ListOpenWorkflowExecutionsRequest,
                        request_bytes
                    )
                }
                "ListClosedWorkflowExecutions" => {
                    rpc_call!(
                        client,
                        list_closed_workflow_executions,
                        ListClosedWorkflowExecutionsRequest,
                        request_bytes
                    )
                }
                "ListWorkflowExecutions" => {
                    rpc_call!(
                        client,
                        list_workflow_executions,
                        ListWorkflowExecutionsRequest,
                        request_bytes
                    )
                }
                "CountWorkflowExecutions" => {
                    rpc_call!(
                        client,
                        count_workflow_executions,
                        CountWorkflowExecutionsRequest,
                        request_bytes
                    )
                }
                "UpdateWorkflowExecution" => {
                    rpc_call!(
                        client,
                        update_workflow_execution,
                        UpdateWorkflowExecutionRequest,
                        request_bytes
                    )
                }
                "PollWorkflowExecutionUpdate" => {
                    rpc_call!(
                        client,
                        poll_workflow_execution_update,
                        PollWorkflowExecutionUpdateRequest,
                        request_bytes
                    )
                }
                _ => Err(format!("Unknown RPC method: {}", rpc_name)),
            }
        }
        4 => {
            // Test service
            match rpc_name {
                "GetCurrentTime" => {
                    // GetCurrentTime takes () not a request type
                    let resp = client
                        .get_current_time(().into_request())
                        .await
                        .map_err(|e| format!("RPC failed: {}", e))?;
                    let mut buf = Vec::new();
                    resp.into_inner()
                        .encode(&mut buf)
                        .map_err(|e| format!("Failed to encode response: {}", e))?;
                    Ok(buf)
                }
                "SleepUntil" => {
                    // For now, skip SleepUntil - it's only needed for time-skipping tests
                    Err("SleepUntil not yet implemented".to_string())
                }
                _ => Err(format!("Unknown test service RPC: {}", rpc_name)),
            }
        }
        _ => Err(format!("Unsupported service: {}", service)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_handle_size() {
        // Ensure ClientHandle is not zero-sized
        assert!(std::mem::size_of::<ClientHandle>() > 0);
    }
}
