#!/usr/bin/env bash
# Generate Crystal protobuf classes from Temporal SDK Core proto files.
#
# Uses protoc + protoc-gen-crystal. Since protoc-gen-crystal generates flat
# filenames and would conflict when processing multiple message.proto files,
# we generate each proto into an isolated temp directory, then move and rename
# the primary output file to a namespaced destination.
#
# Transitive dependency files generated alongside are discarded because they
# will be re-generated correctly when we process the actual dependency directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROTOS_ROOT="${ROOT_DIR}/ext/sdk-core/crates/common/protos"
OUT_DIR="${ROOT_DIR}/src/temporalio/internal/proto"

LOCAL="${PROTOS_ROOT}/local"
API="${PROTOS_ROOT}/api_upstream"
TESTSRV="${PROTOS_ROOT}/testsrv_upstream"

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

TMPDIR="$(mktemp -d)"
trap "rm -rf ${TMPDIR}" EXIT

echo "Generating Crystal protobuf classes..."
echo "Output: ${OUT_DIR}"

# gen <dest_file> <include_flags...> -- <proto_file>
# Generates a single proto file; the primary output is saved as dest_file.
# The proto file's basename (without path) determines the generated filename.
gen() {
  local dest="$1"; shift
  local proto_file="${@: -1}"         # last argument
  local set_args=("${@:1:$#-1}")     # all but last = flags

  local basename
  basename="$(basename "${proto_file}" .proto)"
  local generated="${basename}.pb.cr"

  local tmp_out="${TMPDIR}/$(echo "${dest}" | tr '/' '_')"
  mkdir -p "${tmp_out}"

  protoc "${set_args[@]}" --crystal_out="${tmp_out}" "${proto_file}" 2>/dev/null || true

  if [[ -f "${tmp_out}/${generated}" ]]; then
    mkdir -p "$(dirname "${OUT_DIR}/${dest}")"
    cp "${tmp_out}/${generated}" "${OUT_DIR}/${dest}"
    echo "  OK  ${dest}"
  else
    echo "  SKIP ${dest} (no output: ${generated} not found in ${tmp_out})"
    ls "${tmp_out}" 2>/dev/null || true
  fi
}

# ── SDK Core / local protos ───────────────────────────────────────────────────
gen "coresdk/workflow_activation.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/workflow_activation/workflow_activation.proto"

gen "coresdk/workflow_commands.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/workflow_commands/workflow_commands.proto"

gen "coresdk/workflow_completion.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/workflow_completion/workflow_completion.proto"

gen "coresdk/activity_task.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/activity_task/activity_task.proto"

gen "coresdk/activity_result.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/activity_result/activity_result.proto"

gen "coresdk/child_workflow.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/child_workflow/child_workflow.proto"

gen "coresdk/common.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/common/common.proto"

gen "coresdk/core_interface.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/core_interface.proto"

gen "coresdk/external_data.pb.cr" \
  -I"${LOCAL}" -I"${API}" \
  "${LOCAL}/temporal/sdk/core/external_data/external_data.proto"

# ── Temporal API enums ────────────────────────────────────────────────────────
gen "api/enums/workflow.pb.cr"       -I"${API}" "${API}/temporal/api/enums/v1/workflow.proto"
gen "api/enums/common.pb.cr"         -I"${API}" "${API}/temporal/api/enums/v1/common.proto"
gen "api/enums/failed_cause.pb.cr"   -I"${API}" "${API}/temporal/api/enums/v1/failed_cause.proto"
gen "api/enums/query.pb.cr"          -I"${API}" "${API}/temporal/api/enums/v1/query.proto"
gen "api/enums/event_type.pb.cr"     -I"${API}" "${API}/temporal/api/enums/v1/event_type.proto"
gen "api/enums/task_queue.pb.cr"     -I"${API}" "${API}/temporal/api/enums/v1/task_queue.proto"
gen "api/enums/update.pb.cr"         -I"${API}" "${API}/temporal/api/enums/v1/update.proto"
gen "api/enums/reset.pb.cr"          -I"${API}" "${API}/temporal/api/enums/v1/reset.proto"
gen "api/enums/schedule.pb.cr"       -I"${API}" "${API}/temporal/api/enums/v1/schedule.proto"
gen "api/enums/namespace.pb.cr"      -I"${API}" "${API}/temporal/api/enums/v1/namespace.proto"
gen "api/enums/batch_operation.pb.cr" -I"${API}" "${API}/temporal/api/enums/v1/batch_operation.proto"

# ── Temporal API messages ─────────────────────────────────────────────────────
gen "api/common/message.pb.cr"       -I"${API}" "${API}/temporal/api/common/v1/message.proto"
gen "api/failure/message.pb.cr"      -I"${API}" "${API}/temporal/api/failure/v1/message.proto"
gen "api/taskqueue/message.pb.cr"    -I"${API}" "${API}/temporal/api/taskqueue/v1/message.proto"
gen "api/history/message.pb.cr"      -I"${API}" "${API}/temporal/api/history/v1/message.proto"
gen "api/workflow/message.pb.cr"     -I"${API}" "${API}/temporal/api/workflow/v1/message.proto"
gen "api/filter/message.pb.cr"       -I"${API}" "${API}/temporal/api/filter/v1/message.proto"
gen "api/query/message.pb.cr"        -I"${API}" "${API}/temporal/api/query/v1/message.proto"
gen "api/update/message.pb.cr"       -I"${API}" "${API}/temporal/api/update/v1/message.proto"
gen "api/protocol/message.pb.cr"     -I"${API}" "${API}/temporal/api/protocol/v1/message.proto"
gen "api/activity/message.pb.cr"     -I"${API}" "${API}/temporal/api/activity/v1/message.proto"
gen "api/namespace/message.pb.cr"    -I"${API}" "${API}/temporal/api/namespace/v1/message.proto"
gen "api/schedule/message.pb.cr"     -I"${API}" "${API}/temporal/api/schedule/v1/message.proto"
gen "api/errordetails/message.pb.cr" -I"${API}" "${API}/temporal/api/errordetails/v1/message.proto"

gen "api/workflowservice/request_response.pb.cr" \
  -I"${API}" \
  "${API}/temporal/api/workflowservice/v1/request_response.proto"

gen "api/testservice/request_response.pb.cr" \
  -I"${TESTSRV}" -I"${API}" \
  "${TESTSRV}/temporal/api/testservice/v1/request_response.proto"

echo ""
echo "Done. Files generated:"
find "${OUT_DIR}" -name "*.cr" | sort
