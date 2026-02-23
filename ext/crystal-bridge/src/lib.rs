use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::OnceLock;

mod client;
mod memory;
mod runtime;
mod testing;
mod worker;

pub use client::*;
pub use memory::*;
pub use runtime::*;
pub use testing::*;
pub use worker::*;

/// Initialize the Rust extension
/// This should be called once when the Crystal library is loaded
#[no_mangle]
pub extern "C" fn temporalio_init() -> c_int {
    // Initialize the Tokio runtime
    if runtime::init_runtime().is_err() {
        return -1;
    }
    0
}

/// Get the version string of the bridge
#[no_mangle]
pub extern "C" fn temporalio_version() -> *const c_char {
    static VERSION: OnceLock<CString> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            CString::new(concat!(
                "temporalio-crystal-bridge/",
                env!("CARGO_PKG_VERSION")
            ))
            .unwrap()
        })
        .as_ptr()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        assert_eq!(temporalio_init(), 0);
    }

    #[test]
    fn test_version() {
        let version = unsafe { CStr::from_ptr(temporalio_version()) };
        assert!(version
            .to_str()
            .unwrap()
            .starts_with("temporalio-crystal-bridge/"));
    }
}
