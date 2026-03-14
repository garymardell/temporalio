require "file_utils"

base = File.expand_path("..", __DIR__)
bridge = File.join(base, "ext", "sdk-core")

SDK_CORE_REPO   = "https://github.com/temporalio/sdk-core.git"
SDK_CORE_COMMIT = "43a8a7a8376df33012c650cc5dbbbdb1e55daa6d"

# Clone sdk-core if not present (shards doesn't initialize git submodules)
unless File.exists?(File.join(bridge, "Cargo.toml"))
  puts "Cloning sdk-core..."

  # Try git submodule first (works when .git exists)
  status = Process.run("git", ["submodule", "update", "--init", "--recursive", "ext/sdk-core"], chdir: base)

  unless status.success?
    # Fallback: clone directly (works when installed as a shard without .git)
    FileUtils.rm_rf(bridge)
    status = Process.run("git", ["clone", "--depth", "1", SDK_CORE_REPO, bridge])
    abort "Failed to clone sdk-core" unless status.success?

    status = Process.run("git", ["checkout", SDK_CORE_COMMIT], chdir: bridge)
    unless status.success?
      # Shallow clone may not have the commit, fetch full history
      Process.run("git", ["fetch", "--unshallow"], chdir: bridge)
      status = Process.run("git", ["checkout", SDK_CORE_COMMIT], chdir: bridge)
      abort "Failed to checkout sdk-core commit #{SDK_CORE_COMMIT}" unless status.success?
    end
  end
end

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
status = Process.run(
  "cargo",
  ["build", "-p", "temporalio-sdk-core-c-bridge", "--release"],
  chdir: bridge,
  env: env,
  output: Process::Redirect::Inherit,
  error: Process::Redirect::Inherit
)
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
dest_dir = File.join(base, "ext", "lib", host)
FileUtils.mkdir_p(dest_dir)
FileUtils.cp(src, dest_dir)

puts "Built and copied #{src} -> #{dest_dir}"
