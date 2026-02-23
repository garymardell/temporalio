# Temporal Integration Test Suite

This directory contains comprehensive end-to-end integration tests that validate the Temporal Crystal SDK against a real Temporal server.

## Purpose

These tests verify:

- **Determinism**: Workflows execute deterministically across replays and worker restarts
- **Completion**: All workflow types complete successfully with correct results
- **Fault Tolerance**: System handles worker crashes, activity retries, and failures gracefully
- **Features**: All Temporal features work correctly (signals, queries, timers, activities, child workflows, continue-as-new, cancellation)
- **Load Handling**: System performs correctly under concurrent load
- **Retry Behavior**: Activity and workflow retries work as expected

## Prerequisites

### 1. Install Temporal CLI

```bash
brew install temporal
```

Or download from: https://github.com/temporalio/cli/releases

### 2. Start Temporal Dev Server

```bash
temporal server start-dev
```

This starts:
- Temporal Server on `localhost:7233`
- Web UI on `http://localhost:8233`

Keep this running in a separate terminal during tests.

### 3. Build SDK Dependencies

Ensure the SDK Core bindings are properly built:

```bash
cd ext/sdk-core
cargo build --release
```

## Running the Tests

### Run All Integration Tests

```bash
crystal spec spec/integration/integration_spec.cr
```

### Run Specific Test Groups

```bash
# Basic workflow execution
crystal spec spec/integration/integration_spec.cr -e "Basic Workflow Execution"

# Signals and queries
crystal spec spec/integration/integration_spec.cr -e "Signals and Queries"

# Activity retries
crystal spec spec/integration/integration_spec.cr -e "Activity Execution and Retries"

# Cancellation
crystal spec spec/integration/integration_spec.cr -e "Cancellation"

# Child workflows
crystal spec spec/integration/integration_spec.cr -e "Child Workflows"

# Continue-as-new
crystal spec spec/integration/integration_spec.cr -e "Continue-As-New"

# Worker crash recovery and determinism
crystal spec spec/integration/integration_spec.cr -e "Worker Crash Recovery"

# Stress and load tests
crystal spec spec/integration/integration_spec.cr -e "Stress and Load Testing"
```

### Run with Verbose Output

```bash
crystal spec spec/integration/integration_spec.cr --verbose
```

## Test Coverage

### Basic Workflow Execution (2 tests)
- ✓ Simple workflow completion
- ✓ Workflow with timers

### Signals and Queries (1 test)
- ✓ Signal delivery and state modification
- ✓ Query execution for reading state
- ✓ Multiple signals in sequence

### Activity Execution and Retries (2 tests)
- ✓ Activity retry on failure until success
- ✓ Multiple activities in sequence
- ✓ Activity result propagation

### Cancellation (1 test)
- ✓ Workflow cancellation handling
- ✓ CancelledError exception

### Child Workflows (1 test)
- ✓ Parent-child workflow execution
- ✓ Result propagation from child to parent

### Continue-As-New (1 test)
- ✓ Workflow continuation with new execution
- ✓ State carried forward across runs
- ✓ Multiple continuations in sequence

### Worker Crash Recovery (2 tests)
- ✓ Workflow resumes after worker restart
- ✓ State consistency across crashes
- ✓ Deterministic replay across multiple executions
- ✓ History validation

### Stress and Load Testing (2 tests)
- ✓ 50 concurrent workflow executions
- ✓ Rapid signal delivery (100 signals)
- ✓ Result correctness under load

### Fault Tolerance (3 pending tests)
- ⏸ Activity timeout handling
- ⏸ Workflow execution timeout
- ⏸ Non-retryable activity failures

**Total: 11 active tests, 3 pending**

## Test Workflows and Activities

### Workflows

| Workflow | Purpose |
|----------|---------|
| `SimpleCompletionWorkflow` | Basic workflow execution |
| `TimerWorkflow` | Timer/sleep functionality |
| `SignalAccumulatorWorkflow` | Signal and query handling |
| `CancellationWorkflow` | Cancellation behavior |
| `ChildWorkflowParent/Child` | Parent-child relationships |
| `ContinueAsNewWorkflow` | Continue-as-new pattern |
| `WorkerCrashWorkflow` | Crash recovery simulation |
| `DeterminismTestWorkflow` | Determinism verification |
| `ParallelActivitiesWorkflow` | Multiple activity execution |

### Activities

| Activity | Purpose |
|----------|---------|
| `RetryableActivity` | Tests retry behavior with configurable failure count |
| `DeterministicActivity` | Verifies deterministic activity execution |
| `ParallelActivity` | Tests multiple activity execution |

## Monitoring Tests

### Using Temporal Web UI

1. Open http://localhost:8233 in your browser
2. Navigate to "Workflows" section
3. Filter by workflow ID pattern (e.g., `simple-*`, `crash-*`)
4. View workflow history, events, and execution details

### Using Temporal CLI

```bash
# List recent workflows
temporal workflow list

# Describe a specific workflow
temporal workflow describe --workflow-id <workflow-id>

# Show workflow history
temporal workflow show --workflow-id <workflow-id>

# Query a running workflow
temporal workflow query --workflow-id <workflow-id> --type get_values
```

## Debugging Failed Tests

### 1. Check Temporal Server Logs

The dev server shows logs in the terminal where it's running.

### 2. View Workflow History

```bash
temporal workflow show --workflow-id <workflow-id>
```

### 3. Enable SDK Logging

Set environment variable for verbose SDK logging:

```bash
TEMPORAL_DEBUG=1 crystal spec spec/integration/integration_spec.cr
```

### 4. Increase Test Timeouts

If tests fail with timeouts, increase the workflow execution timeout in the test:

```crystal
workflow_execution_timeout: 60.seconds  # increase from 30s
```

### 5. Check for Port Conflicts

Ensure port 7233 is not in use by another service:

```bash
lsof -i :7233
```

## Common Issues

### "Connection refused" errors

**Cause**: Temporal server not running

**Solution**: Start the dev server with `temporal server start-dev`

### Tests timeout

**Cause**: Worker not processing tasks

**Solution**: 
- Check worker is created and started correctly
- Verify workflow/activity classes are registered
- Check task queue names match between client and worker

### "Workflow not found" errors

**Cause**: Workflow hasn't started yet or wrong workflow ID

**Solution**:
- Add `sleep 200.milliseconds` after starting workflow before querying/signaling
- Verify workflow ID matches

### Determinism errors

**Cause**: Non-deterministic code in workflow (random values, time, I/O)

**Solution**:
- Use `Temporalio::Workflow.now` instead of `Time.utc`
- Use `Temporalio::Workflow.random` for random values (TODO: implement)
- Move side effects to activities

## Test Scenarios Explained

### Worker Crash Recovery Test

This test simulates a worker crash and restart:

1. Start worker #1 and begin workflow execution
2. Workflow progresses through steps 1-3 (with timers)
3. Worker #1 is shut down (simulates crash)
4. Start worker #2 (simulates restart)
5. Worker #2 picks up the workflow and replays history
6. Workflow continues from step 3 onwards
7. Verifies final result is correct

**What this proves**: Temporal correctly stores workflow state in the server, and workers can resume execution after crashes without data loss.

### Determinism Test

This test runs a workflow with multiple timers and activities:

1. Execute workflow with 3 iterations
2. Each iteration: timer → activity
3. Verify result contains all expected values
4. Workflow will be replayed multiple times during execution
5. Each replay must produce identical commands

**What this proves**: The workflow execution is deterministic - replaying history produces the same sequence of commands every time.

### Rapid Signal Test

This test sends 100 signals rapidly to a single workflow:

1. Start workflow that accumulates signal values
2. Send 100 signals as fast as possible
3. Query to verify all signals received
4. Complete workflow and verify final state

**What this proves**: Temporal correctly handles rapid signal delivery without loss, and signals are processed in order.

## Performance Benchmarks

Expected execution times on modern hardware:

- Simple workflow: < 100ms
- Timer workflow (200ms sleep): ~200-300ms
- Activity retry (3 attempts): ~500ms
- 50 concurrent workflows: < 10s
- 100 rapid signals: < 2s
- Worker crash recovery: < 5s

## Contributing

When adding new integration tests:

1. Create workflow/activity files in `workflows/` or `activities/`
2. Add test case to `integration_spec.cr`
3. Use unique task queues per test: `"test-#{feature}-#{Random.rand(10000)}"`
4. Use unique workflow IDs: `unique_id("prefix")`
5. Always shut down workers in `ensure` block
6. Add documentation to this README

## License

Same as parent project.
