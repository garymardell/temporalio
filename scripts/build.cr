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
    status = Process.run("git", ["clone", SDK_CORE_REPO, bridge],
      output: Process::Redirect::Inherit,
      error: Process::Redirect::Inherit)
    abort "Failed to clone sdk-core" unless status.success?

    status = Process.run("git", ["checkout", SDK_CORE_COMMIT], chdir: bridge,
      output: Process::Redirect::Inherit,
      error: Process::Redirect::Inherit)
    abort "Failed to checkout sdk-core commit #{SDK_CORE_COMMIT}" unless status.success?
  end
end

# Build cdylib
puts "Building temporalio-sdk-core-c-bridge..."
status = Process.run(
  "cargo",
  ["build", "-p", "temporalio-sdk-core-c-bridge", "--release"],
  chdir: bridge,
  output: Process::Redirect::Inherit,
  error: Process::Redirect::Inherit
)
abort "cargo build failed" unless status.success?

puts "Build complete"
