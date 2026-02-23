use std::os::raw::{c_char, c_void};
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfig,
    TemporalDevServerConfigBuilder, TestServerConfig, TestServerConfigBuilder,
};

use crate::memory::ByteArray;
use crate::runtime::get_runtime;

/// Options for starting a test server
#[repr(C)]
pub struct TestServerOptions {
    pub existing_path: *const c_char,
    pub sdk_name: *const c_char,
    pub sdk_version: *const c_char,
    pub download_version: *const c_char,
    pub download_dest_dir: *const c_char,
    pub port: u16,
    pub extra_args: *const c_char,
    pub download_ttl_seconds: u64,
}

/// Options for starting a dev server
#[repr(C)]
pub struct DevServerOptions {
    pub test_server: *const TestServerOptions,
    pub namespace: *const c_char,
    pub ip: *const c_char,
    pub database_filename: *const c_char,
    pub ui: bool,
    pub ui_port: u16,
    pub log_format: *const c_char,
    pub log_level: *const c_char,
}

/// Opaque handle to an ephemeral server instance
pub struct EphemeralServerHandle {
    server: Option<EphemeralServer>,
}

/// Callback type for ephemeral server start operations
pub type EphemeralServerStartCallback = unsafe extern "C" fn(
    user_data: *mut c_void,
    success: *mut EphemeralServerHandle,
    success_target: *const ByteArray,
    fail: *const ByteArray,
);

/// Callback type for ephemeral server shutdown operations
pub type EphemeralServerShutdownCallback =
    unsafe extern "C" fn(user_data: *mut c_void, fail: *const ByteArray);

impl TestServerOptions {
    unsafe fn to_config(&self) -> anyhow::Result<TestServerConfig> {
        let exe = if !self.existing_path.is_null() {
            let path = std::ffi::CStr::from_ptr(self.existing_path)
                .to_str()?
                .to_string();
            EphemeralExe::ExistingPath(path)
        } else {
            let sdk_name = if !self.sdk_name.is_null() {
                std::ffi::CStr::from_ptr(self.sdk_name)
                    .to_str()?
                    .to_string()
            } else {
                "sdk-crystal".to_string()
            };

            let sdk_version = if !self.sdk_version.is_null() {
                std::ffi::CStr::from_ptr(self.sdk_version)
                    .to_str()?
                    .to_string()
            } else {
                env!("CARGO_PKG_VERSION").to_string()
            };

            let version = if !self.download_version.is_null() {
                let download_version = std::ffi::CStr::from_ptr(self.download_version)
                    .to_str()?
                    .to_string();
                if download_version == "default" {
                    EphemeralExeVersion::SDKDefault {
                        sdk_name,
                        sdk_version,
                    }
                } else {
                    EphemeralExeVersion::Fixed(download_version)
                }
            } else {
                EphemeralExeVersion::SDKDefault {
                    sdk_name,
                    sdk_version,
                }
            };

            let dest_dir = if !self.download_dest_dir.is_null() {
                Some(
                    std::ffi::CStr::from_ptr(self.download_dest_dir)
                        .to_str()?
                        .to_string(),
                )
            } else {
                None
            };

            let ttl = if self.download_ttl_seconds == 0 {
                None
            } else {
                Some(std::time::Duration::from_secs(self.download_ttl_seconds))
            };

            EphemeralExe::CachedDownload {
                version,
                dest_dir,
                ttl,
            }
        };

        let port = if self.port == 0 {
            None
        } else {
            Some(self.port)
        };

        let extra_args = if !self.extra_args.is_null() {
            let args_str = std::ffi::CStr::from_ptr(self.extra_args).to_str()?;
            args_str
                .split('\n')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        } else {
            Vec::new()
        };

        Ok(TestServerConfigBuilder::default()
            .exe(exe)
            .port(port)
            .extra_args(extra_args)
            .build()?)
    }
}

impl DevServerOptions {
    unsafe fn to_config(&self) -> anyhow::Result<TemporalDevServerConfig> {
        let test_server_options = &*self.test_server;

        let exe = if !test_server_options.existing_path.is_null() {
            let path = std::ffi::CStr::from_ptr(test_server_options.existing_path)
                .to_str()?
                .to_string();
            EphemeralExe::ExistingPath(path)
        } else {
            let sdk_name = if !test_server_options.sdk_name.is_null() {
                std::ffi::CStr::from_ptr(test_server_options.sdk_name)
                    .to_str()?
                    .to_string()
            } else {
                "sdk-crystal".to_string()
            };

            let sdk_version = if !test_server_options.sdk_version.is_null() {
                std::ffi::CStr::from_ptr(test_server_options.sdk_version)
                    .to_str()?
                    .to_string()
            } else {
                env!("CARGO_PKG_VERSION").to_string()
            };

            let version = if !test_server_options.download_version.is_null() {
                let download_version =
                    std::ffi::CStr::from_ptr(test_server_options.download_version)
                        .to_str()?
                        .to_string();
                if download_version == "default" {
                    EphemeralExeVersion::SDKDefault {
                        sdk_name,
                        sdk_version,
                    }
                } else {
                    EphemeralExeVersion::Fixed(download_version)
                }
            } else {
                EphemeralExeVersion::SDKDefault {
                    sdk_name,
                    sdk_version,
                }
            };

            let dest_dir = if !test_server_options.download_dest_dir.is_null() {
                Some(
                    std::ffi::CStr::from_ptr(test_server_options.download_dest_dir)
                        .to_str()?
                        .to_string(),
                )
            } else {
                None
            };

            let ttl = if test_server_options.download_ttl_seconds == 0 {
                None
            } else {
                Some(std::time::Duration::from_secs(
                    test_server_options.download_ttl_seconds,
                ))
            };

            EphemeralExe::CachedDownload {
                version,
                dest_dir,
                ttl,
            }
        };

        let namespace = if !self.namespace.is_null() {
            std::ffi::CStr::from_ptr(self.namespace)
                .to_str()?
                .to_string()
        } else {
            "default".to_string()
        };

        let ip = if !self.ip.is_null() {
            std::ffi::CStr::from_ptr(self.ip).to_str()?.to_string()
        } else {
            "127.0.0.1".to_string()
        };

        let database_filename = if !self.database_filename.is_null() {
            Some(
                std::ffi::CStr::from_ptr(self.database_filename)
                    .to_str()?
                    .to_string(),
            )
        } else {
            None
        };

        let ui_port = if self.ui_port == 0 || !self.ui {
            None
        } else {
            Some(self.ui_port)
        };

        let log_format = if !self.log_format.is_null() {
            std::ffi::CStr::from_ptr(self.log_format)
                .to_str()?
                .to_string()
        } else {
            "pretty".to_string()
        };

        let log_level = if !self.log_level.is_null() {
            std::ffi::CStr::from_ptr(self.log_level)
                .to_str()?
                .to_string()
        } else {
            "warn".to_string()
        };

        let port = if test_server_options.port == 0 {
            None
        } else {
            Some(test_server_options.port)
        };

        let extra_args = if !test_server_options.extra_args.is_null() {
            let args_str = std::ffi::CStr::from_ptr(test_server_options.extra_args).to_str()?;
            args_str
                .split('\n')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        } else {
            Vec::new()
        };

        Ok(TemporalDevServerConfigBuilder::default()
            .exe(exe)
            .namespace(namespace)
            .ip(ip)
            .port(port)
            .db_filename(database_filename)
            .ui(self.ui)
            .ui_port(ui_port)
            .log((log_format, log_level))
            .extra_args(extra_args)
            .build()?)
    }
}

/// Start a test server (blocking call)
#[no_mangle]
pub unsafe extern "C" fn temporalio_ephemeral_server_start_test_server(
    options: *const TestServerOptions,
    user_data: *mut c_void,
    callback: EphemeralServerStartCallback,
) {
    let options = match (*options).to_config() {
        Ok(config) => config,
        Err(e) => {
            let error_msg = format!("Failed to parse test server options: {}", e);
            let error_bytes = ByteArray::from_string(error_msg);
            callback(
                user_data,
                std::ptr::null_mut(),
                std::ptr::null(),
                &error_bytes,
            );
            return;
        }
    };

    let result = get_runtime().block_on(async move { options.start_server().await });

    match result {
        Ok(server) => {
            let target = server.target.clone();
            let server_handle = Box::new(EphemeralServerHandle {
                server: Some(server),
            });
            let target_bytes = ByteArray::from_string(target);

            callback(
                user_data,
                Box::into_raw(server_handle),
                &target_bytes,
                std::ptr::null(),
            );
        }
        Err(e) => {
            let error_msg = format!("Failed to start test server: {}", e);
            let error_bytes = ByteArray::from_string(error_msg);

            callback(
                user_data,
                std::ptr::null_mut(),
                std::ptr::null(),
                &error_bytes,
            );
        }
    }
}

/// Start a dev server (blocking call)
#[no_mangle]
pub unsafe extern "C" fn temporalio_ephemeral_server_start_dev_server(
    options: *const DevServerOptions,
    user_data: *mut c_void,
    callback: EphemeralServerStartCallback,
) {
    let options = match (*options).to_config() {
        Ok(config) => config,
        Err(e) => {
            let error_msg = format!("Failed to parse dev server options: {}", e);
            let error_bytes = ByteArray::from_string(error_msg);
            callback(
                user_data,
                std::ptr::null_mut(),
                std::ptr::null(),
                &error_bytes,
            );
            return;
        }
    };

    let result = get_runtime().block_on(async move { options.start_server().await });

    match result {
        Ok(server) => {
            let target = server.target.clone();
            let server_handle = Box::new(EphemeralServerHandle {
                server: Some(server),
            });
            let target_bytes = ByteArray::from_string(target);

            callback(
                user_data,
                Box::into_raw(server_handle),
                &target_bytes,
                std::ptr::null(),
            );
        }
        Err(e) => {
            let error_msg = format!("Failed to start dev server: {}", e);
            let error_bytes = ByteArray::from_string(error_msg);

            callback(
                user_data,
                std::ptr::null_mut(),
                std::ptr::null(),
                &error_bytes,
            );
        }
    }
}

/// Shutdown an ephemeral server (blocking call)
#[no_mangle]
pub unsafe extern "C" fn temporalio_ephemeral_server_shutdown(
    server: *mut EphemeralServerHandle,
    user_data: *mut c_void,
    callback: EphemeralServerShutdownCallback,
) {
    let mut server_handle = Box::from_raw(server);

    let mut eph_server: EphemeralServer = match server_handle.server.take() {
        Some(s) => s,
        None => {
            let error_msg = "Server already shutdown".to_string();
            let error_bytes = ByteArray::from_string(error_msg);
            callback(user_data, &error_bytes);
            return;
        }
    };

    let result = get_runtime().block_on(async move { eph_server.shutdown().await });

    match result {
        Ok(_) => {
            callback(user_data, std::ptr::null());
        }
        Err(e) => {
            let error_msg = format!("Failed to shutdown server: {}", e);
            let error_bytes = ByteArray::from_string(error_msg);
            callback(user_data, &error_bytes);
        }
    }
}

/// Free an ephemeral server handle
#[no_mangle]
pub unsafe extern "C" fn temporalio_ephemeral_server_free(server: *mut EphemeralServerHandle) {
    if !server.is_null() {
        let _ = Box::from_raw(server);
    }
}
