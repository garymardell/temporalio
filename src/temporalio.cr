module Temporalio
  VERSION = "0.1.0"
end

# NOTE: lib_temporal (C bridge) is only required for WorkflowReplayer
# All other functionality uses the Rust extension in ext/
require "./lib_temporal"
require "./temporalio/internal/proto"
require "./temporalio/exceptions"
require "./temporalio/data_converter"
require "./temporalio/payload_converter"
require "./temporalio/fast_payload_converter"
require "./temporalio/bridge"
require "./temporalio/client"
require "./temporalio/activity"
require "./temporalio/activity/context"
require "./temporalio/activity/activity_info"
require "./temporalio/worker"
require "./temporalio/workflow"
require "./temporalio/internal/activity_runner"
require "./temporalio/internal/failure_converter"
require "./temporalio/internal/workflow_instance"
require "./temporalio/internal/workflow_runner"
require "./temporalio/interceptor/client_interceptor"
require "./temporalio/interceptor/worker_interceptor"
require "./temporalio/testing/activity_environment"
require "./temporalio/testing/workflow_environment"
require "./temporalio/testing/workflow_replayer"
