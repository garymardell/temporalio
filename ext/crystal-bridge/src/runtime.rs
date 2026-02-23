use std::sync::OnceLock;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};
use tokio::runtime::Runtime;

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static CORE_RUNTIME: OnceLock<CoreRuntime> = OnceLock::new();

/// Initialize the global Tokio runtime
pub fn init_runtime() -> Result<(), String> {
    if TOKIO_RUNTIME.get().is_some() {
        // Already initialized, that's fine
        return Ok(());
    }

    TOKIO_RUNTIME
        .set(Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?)
        .map_err(|_| "Runtime already initialized".to_string())
}

/// Get a reference to the global Tokio runtime
pub fn get_runtime() -> &'static Runtime {
    TOKIO_RUNTIME
        .get()
        .expect("Runtime not initialized - call temporalio_init() first")
}

/// Initialize the CoreRuntime for Worker support
pub fn init_core_runtime() -> Result<(), String> {
    if CORE_RUNTIME.get().is_some() {
        return Ok(());
    }

    // Get or create the Tokio runtime first
    if TOKIO_RUNTIME.get().is_none() {
        init_runtime()?;
    }

    let runtime = get_runtime();

    // Create CoreRuntime using the Tokio runtime
    let core_runtime = runtime
        .block_on(async { CoreRuntime::new_assume_tokio(RuntimeOptions::default()) })
        .map_err(|e| format!("Failed to create CoreRuntime: {}", e))?;

    CORE_RUNTIME
        .set(core_runtime)
        .map_err(|_| "CoreRuntime already initialized".to_string())
}

/// Get a reference to the global CoreRuntime
pub fn get_core_runtime() -> Result<&'static CoreRuntime, String> {
    CORE_RUNTIME
        .get()
        .ok_or_else(|| "CoreRuntime not initialized - call init_core_runtime() first".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_initialization() {
        assert!(init_runtime().is_ok());
        let runtime = get_runtime();
        assert!(runtime.block_on(async { true }));
    }

    #[test]
    fn test_core_runtime_initialization() {
        assert!(init_core_runtime().is_ok());
        assert!(get_core_runtime().is_ok());
    }
}
