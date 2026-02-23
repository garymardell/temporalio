require "file_utils"

base = File.expand_path("..", __DIR__)
bridge = File.join(base, "..", "ext", "sdk-core")

# Figure out target triple-ish folder & output filenames
host =
  case {{ flag?(:win32) }}
  when true then "windows"
  else
    case {{ flag?(:darwin) }}
    when true then "darwin"
    else           "linux"
    end
  end

# Choose cargo target dir inside the repo for reproducibility
env = {"CARGO_TERM_COLOR" => "never"} of String => String
release_dir = File.join(bridge, "target", "release")

# Build cdylib
cmd = %(cargo build -p temporalio-sdk-core-c-bridge --release --manifest-path ext/sdk-core/Cargo.toml)
status = Process.run(cmd, chdir: bridge, env: env)
abort "cargo build failed" unless status.success?

# Determine produced filenames
libname = "temporal_core"
src =
  case host
  when "darwin" then File.join(release_dir, "lib#{libname}.dylib")
  when "linux"  then File.join(release_dir, "lib#{libname}.so")
  else               File.join(release_dir, "#{libname}.dll")
  end

# Copy into a known location your shard can load from at runtime
dest_dir = File.join(base, "..", "ext", "lib", host)
FileUtils.mkdir_p(dest_dir)
FileUtils.cp(src, dest_dir)

puts "Built and copied #{src} -> #{dest_dir}"
