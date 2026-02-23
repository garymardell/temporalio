use std::slice;

/// Represents a byte array that can be passed across the FFI boundary
/// Crystal owns the input data, Rust owns the output data
#[repr(C)]
pub struct ByteArray {
    pub data: *mut u8,
    pub len: usize,
}

impl ByteArray {
    /// Create a ByteArray from a Vec<u8>, transferring ownership to the caller
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let mut boxed = vec.into_boxed_slice();
        let len = boxed.len();
        let data = boxed.as_mut_ptr();
        std::mem::forget(boxed); // Prevent Rust from freeing
        ByteArray { data, len }
    }

    /// Create an empty ByteArray
    pub fn empty() -> Self {
        ByteArray {
            data: std::ptr::null_mut(),
            len: 0,
        }
    }

    /// Create a ByteArray from a String
    pub fn from_string(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }

    /// Check if the ByteArray is null/empty
    pub fn is_empty(&self) -> bool {
        self.data.is_null() || self.len == 0
    }

    /// Convert to a slice (doesn't transfer ownership)
    pub unsafe fn as_slice(&self) -> &[u8] {
        if self.is_empty() {
            &[]
        } else {
            slice::from_raw_parts(self.data, self.len)
        }
    }
}

/// Free a ByteArray allocated by Rust
/// This should be called from Crystal when done with the data
#[no_mangle]
pub extern "C" fn temporalio_byte_array_free(array: ByteArray) {
    if !array.data.is_null() && array.len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(array.data, array.len, array.len);
            // Vec will be dropped here, freeing the memory
        }
    }
}

/// Represents an error that can be passed across the FFI boundary
#[repr(C)]
pub struct TemporalioError {
    pub code: i32,
    pub message: ByteArray,
}

impl TemporalioError {
    pub fn new(code: i32, message: String) -> Self {
        TemporalioError {
            code,
            message: ByteArray::from_vec(message.into_bytes()),
        }
    }

    pub fn null() -> Self {
        TemporalioError {
            code: 0,
            message: ByteArray::empty(),
        }
    }
}

/// Free a TemporalioError allocated by Rust
#[no_mangle]
pub extern "C" fn temporalio_error_free(error: TemporalioError) {
    temporalio_byte_array_free(error.message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_array_from_vec() {
        let vec = vec![1, 2, 3, 4, 5];
        let array = ByteArray::from_vec(vec);
        assert_eq!(array.len, 5);
        assert!(!array.data.is_null());

        let slice = unsafe { array.as_slice() };
        assert_eq!(slice, &[1, 2, 3, 4, 5]);

        // Clean up
        temporalio_byte_array_free(array);
    }

    #[test]
    fn test_error_creation() {
        let error = TemporalioError::new(-1, "Test error".to_string());
        assert_eq!(error.code, -1);
        assert_eq!(error.message.len, 10);

        let message = unsafe { String::from_utf8_lossy(error.message.as_slice()).to_string() };
        assert_eq!(message, "Test error");

        // Clean up
        temporalio_error_free(error);
    }
}
