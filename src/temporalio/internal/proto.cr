require "protobuf"

# Google Well-Known Types (not bundled with protobuf.cr)
require "./proto/google_wkt.pb"

# API types — load enums first (no dependencies), then common, then failure, then update
require "./proto/api/enums.pb"
require "./proto/api/common.pb"
require "./proto/api/failure.pb"
require "./proto/api/update.pb"
require "./proto/api/workflowservice.pb"

# CoreSDK types
require "./proto/coresdk/common.pb"
require "./proto/coresdk/activity_result.pb"
require "./proto/coresdk/activity_task.pb"
require "./proto/coresdk/child_workflow.pb"
require "./proto/coresdk/workflow_commands.pb"
require "./proto/coresdk/workflow_activation.pb"
require "./proto/coresdk/workflow_completion.pb"
require "./proto/coresdk/core_interface.pb"
