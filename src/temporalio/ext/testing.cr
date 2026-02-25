require "./lib"

module Temporalio
  module Ext
    # Bindings for ephemeral server testing functionality
    module Testing
      # Options for starting a test server
      @[Extern]
      struct TestServerOptions
        property existing_path : LibC::Char*
        property sdk_name : LibC::Char*
        property sdk_version : LibC::Char*
        property download_version : LibC::Char*
        property download_dest_dir : LibC::Char*
        property port : UInt16
        property extra_args : LibC::Char*
        property download_ttl_seconds : UInt64

        def initialize
          @existing_path = Pointer(LibC::Char).null
          @sdk_name = Pointer(LibC::Char).null
          @sdk_version = Pointer(LibC::Char).null
          @download_version = Pointer(LibC::Char).null
          @download_dest_dir = Pointer(LibC::Char).null
          @port = 0_u16
          @extra_args = Pointer(LibC::Char).null
          @download_ttl_seconds = 0_u64
        end
      end

      # Options for starting a dev server
      @[Extern]
      struct DevServerOptions
        property test_server : TestServerOptions*
        property namespace : LibC::Char*
        property ip : LibC::Char*
        property database_filename : LibC::Char*
        property ui : Bool
        property ui_port : UInt16
        property log_format : LibC::Char*
        property log_level : LibC::Char*

        def initialize
          @test_server = Pointer(TestServerOptions).null
          @namespace = Pointer(LibC::Char).null
          @ip = Pointer(LibC::Char).null
          @database_filename = Pointer(LibC::Char).null
          @ui = false
          @ui_port = 0_u16
          @log_format = Pointer(LibC::Char).null
          @log_level = Pointer(LibC::Char).null
        end
      end

      # Opaque handle to an ephemeral server
      alias EphemeralServerHandle = Void*

      # ByteArray from the main lib
      alias ByteArray = LibTemporalioExt::ByteArray

      # Callback types  
      alias StartCallback = Proc(Void*, EphemeralServerHandle, ByteArray*, ByteArray*, Nil)
      alias ShutdownCallback = Proc(Void*, ByteArray*, Nil)

      # Crystal-friendly wrapper for test server configuration
      class TestServer
        getter existing_path : String?
        getter sdk_name : String
        getter sdk_version : String
        getter download_version : String
        getter download_dest_dir : String?
        getter port : UInt16?
        getter extra_args : Array(String)
        getter download_ttl_seconds : UInt64

        def initialize(
          @existing_path : String? = nil,
          @sdk_name : String = "sdk-crystal",
          @sdk_version : String = "0.1.0",
          @download_version : String = "default",
          @download_dest_dir : String? = nil,
          @port : UInt16? = nil,
          @extra_args : Array(String) = [] of String,
          @download_ttl_seconds : UInt64 = 60_u64 * 60_u64 * 24_u64 * 15_u64 # 15 days
        )
        end

        # Start the test server
        def start : EphemeralServer
          Ext.init
          options = build_ffi_options
          start_internal(options)
        end

        # Called by DevServer to get the FFI options struct
        protected def build_ffi_options_for_dev_server : TestServerOptions
          build_ffi_options
        end

        private def build_ffi_options : TestServerOptions
          options = TestServerOptions.new
          
          if path = @existing_path
            options.existing_path = path.to_unsafe
          end
          
          options.sdk_name = @sdk_name.to_unsafe
          options.sdk_version = @sdk_version.to_unsafe
          options.download_version = @download_version.to_unsafe
          
          if dest_dir = @download_dest_dir
            options.download_dest_dir = dest_dir.to_unsafe
          end
          
          options.port = @port || 0_u16
          
          unless @extra_args.empty?
            options.extra_args = @extra_args.join('\n').to_unsafe
          end
          
          options.download_ttl_seconds = @download_ttl_seconds
          
          options
        end

        private def cleanup_ffi_options(options : TestServerOptions)
          # Crystal strings are automatically managed by GC, no need to free
        end

        private def start_internal(options : TestServerOptions) : EphemeralServer
          pipe_r, pipe_w = IO.pipe
          results = [] of ServerStartResult

          state = {pipe_w_fd: pipe_w.fd, results: results}
          box = Box.new(state)

          callback = StartCallback.new do |user_data, server_ptr, target_ptr, error_ptr|
            s = Box(typeof(state)).unbox(user_data)
            r = if !error_ptr.null?
              error = String.new(error_ptr.value.data, error_ptr.value.len)
              ServerStartResult.new(error: error)
            elsif !server_ptr.null? && !target_ptr.null?
              target = String.new(target_ptr.value.data, target_ptr.value.len)
              ServerStartResult.new(server: server_ptr, target: target)
            else
              ServerStartResult.new(error: "Unknown error starting server")
            end
            s[:results] << r
            byte : UInt8 = 1
            LibC.write(s[:pipe_w_fd], pointerof(byte).as(Void*), 1)
          end

          Lib.ephemeral_server_start_test_server(pointerof(options), box.as(Void*), callback)
          pipe_r.read_byte
          pipe_r.close
          pipe_w.close

          r = results.first
          if error = r.error
            raise RuntimeError.new("Failed to start test server: #{error}")
          elsif server = r.server
            EphemeralServer.new(server.not_nil!, r.target.not_nil!)
          else
            raise RuntimeError.new("Unknown error starting test server")
          end
        end

      end

      # Shared result struct for server start operations
      private struct ServerStartResult
        property server : EphemeralServerHandle?
        property target : String?
        property error : String?

        def initialize(@server = nil, @target = nil, @error = nil)
        end
      end

      # Crystal-friendly wrapper for dev server configuration
      class DevServer
        getter test_server : TestServer
        getter namespace : String
        getter ip : String
        getter database_filename : String?
        getter ui : Bool
        getter ui_port : UInt16?
        getter log_format : String
        getter log_level : String

        def initialize(
          @test_server : TestServer = TestServer.new,
          @namespace : String = "default",
          @ip : String = "127.0.0.1",
          @database_filename : String? = nil,
          @ui : Bool = false,
          @ui_port : UInt16? = nil,
          @log_format : String = "pretty",
          @log_level : String = "warn"
        )
        end

        # Start the dev server
        def start : EphemeralServer
          test_options = @test_server.build_ffi_options_for_dev_server
          options = build_ffi_options(test_options)
          start_internal(options, test_options)
        end

        private def build_ffi_options(test_options : TestServerOptions) : DevServerOptions
          options = DevServerOptions.new
          options.test_server = pointerof(test_options)
          options.namespace = @namespace.to_unsafe
          options.ip = @ip.to_unsafe
          
          if db = @database_filename
            options.database_filename = db.to_unsafe
          end
          
          options.ui = @ui
          options.ui_port = @ui_port || 0_u16
          options.log_format = @log_format.to_unsafe
          options.log_level = @log_level.to_unsafe
          
          options
        end

        private def start_internal(options : DevServerOptions, test_options : TestServerOptions) : EphemeralServer
          pipe_r, pipe_w = IO.pipe
          results = [] of ServerStartResult

          state = {pipe_w_fd: pipe_w.fd, results: results}
          box = Box.new(state)

          callback = StartCallback.new do |user_data, server_ptr, target_ptr, error_ptr|
            s = Box(typeof(state)).unbox(user_data)
            r = if !error_ptr.null?
              error = String.new(error_ptr.value.data, error_ptr.value.len)
              ServerStartResult.new(error: error)
            elsif !server_ptr.null? && !target_ptr.null?
              target = String.new(target_ptr.value.data, target_ptr.value.len)
              ServerStartResult.new(server: server_ptr, target: target)
            else
              ServerStartResult.new(error: "Unknown error starting server")
            end
            s[:results] << r
            byte : UInt8 = 1
            LibC.write(s[:pipe_w_fd], pointerof(byte).as(Void*), 1)
          end

          Lib.ephemeral_server_start_dev_server(pointerof(options), box.as(Void*), callback)
          pipe_r.read_byte
          pipe_r.close
          pipe_w.close

          r = results.first
          if error = r.error
            raise RuntimeError.new("Failed to start dev server: #{error}")
          elsif server = r.server
            EphemeralServer.new(server.not_nil!, r.target.not_nil!)
          else
            raise RuntimeError.new("Unknown error starting dev server")
          end
        end
      end

      # Represents a running ephemeral server
      class EphemeralServer
        getter target : String

        def initialize(@handle : EphemeralServerHandle, @target : String)
          @shutdown = false
        end

        # Shutdown the server
        def shutdown
          return if @shutdown

          pipe_r, pipe_w = IO.pipe
          errors = [] of String

          state = {pipe_w_fd: pipe_w.fd, errors: errors}
          box = Box.new(state)

          callback = ShutdownCallback.new do |user_data, error_ptr|
            s = Box(typeof(state)).unbox(user_data)
            if !error_ptr.null?
              s[:errors] << String.new(error_ptr.value.data, error_ptr.value.len)
            end
            byte : UInt8 = 1
            LibC.write(s[:pipe_w_fd], pointerof(byte).as(Void*), 1)
          end

          Lib.ephemeral_server_shutdown(@handle, box.as(Void*), callback)
          pipe_r.read_byte
          pipe_r.close
          pipe_w.close

          @shutdown = true

          if e = errors.first?
            raise RuntimeError.new("Failed to shutdown server: #{e}")
          end
        end

        # Automatically shutdown when finalized
        def finalize
          shutdown unless @shutdown
        rescue
          # Ignore errors during finalization
        end
      end

      # FFI bindings to Rust extension
      @[Link(ldflags: "#{__DIR__}/../../../ext/crystal-bridge/target/release/libtemporalio_crystal.dylib")]
      lib Lib
        # Start a test server
        fun ephemeral_server_start_test_server = temporalio_ephemeral_server_start_test_server(
          options : TestServerOptions*,
          user_data : Void*,
          callback : StartCallback
        ) : Void

        # Start a dev server
        fun ephemeral_server_start_dev_server = temporalio_ephemeral_server_start_dev_server(
          options : DevServerOptions*,
          user_data : Void*,
          callback : StartCallback
        ) : Void

        # Shutdown a server
        fun ephemeral_server_shutdown = temporalio_ephemeral_server_shutdown(
          server : EphemeralServerHandle,
          user_data : Void*,
          callback : ShutdownCallback
        ) : Void

        # Free a server handle
        fun ephemeral_server_free = temporalio_ephemeral_server_free(
          server : EphemeralServerHandle
        ) : Void
      end
    end
  end
end
