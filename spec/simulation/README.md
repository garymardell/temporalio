# Temporal SDK Comprehensive Simulation

This simulation provides a complete, fault-tolerant, deterministic test suite for the Temporal.io Crystal SDK.

## Overview

The simulation exercises all Temporal features in a realistic, production-like environment:

- **Orchestrator Workflow**: Coordinates the entire simulation
- **Scenario Workflows**: Test specific patterns (child workflows, signals, retries, etc.)
- **Activities**: Simulate real-world operations with controlled failures
- **Metrics**: Track execution and validate determinism

## Features Tested

### Core Workflow Patterns
- ✅ Simple workflow execution
- ✅ Child workflows with deep nesting (configurable depth)
- ✅ Continue-as-new for long-running processes
- ✅ Workflow cancellation
- ✅ Timer/sleep operations

### Signal & Query Patterns
- ✅ Signal handling
- ✅ Query execution
- ✅ Dynamic signal injection
- ✅ State management

### Activity Patterns
- ✅ Activity execution
- ✅ Activity timeouts
- ✅ Activity retries with exponential backoff
- ✅ Activity cancellation
- ✅ Heartbeat support
- ✅ Non-retryable failures

### Fault Tolerance
- ✅ Controlled failure injection
- ✅ Timeout scenarios
- ✅ Worker crashes and replay
- ✅ Non-determinism detection
- ✅ Error propagation

### Concurrency & Performance
- ✅ Concurrent workflow execution
- ✅ Parallel activity execution
- ✅ High-throughput signal delivery
- ✅ Load testing

## Configuration

### SimulationConfig

```crystal
config = SimulationConfig.new(
  duration: 60.seconds,           # Total simulation time
  num_workflows: 20,               # Concurrent workflows
  activities_per_workflow: 5,      # Activities per workflow
  failure_rate: 0.2,               # 20% failure rate
  timeout_rate: 0.1,               # 10% timeout rate
  worker_restart_interval: 15.seconds,
  enable_child_workflows: true,
  enable_continue_as_new: true,
  enable_signals: true,
  enable_queries: true,
  enable_cancellation: true,
  max_workflow_depth: 3            # Max child workflow nesting
)
```

## Running the Simulation

### Full Simulation
```bash
crystal spec spec/simulation/simulation_spec.cr
```

### Specific Scenarios
```bash
# Test child workflows only
crystal spec spec/simulation/simulation_spec.cr -e "targeted scenario tests"

# Test determinism
crystal spec spec/simulation/simulation_spec.cr -e "validates determinism"

# Test concurrent execution
crystal spec spec/simulation/simulation_spec.cr -e "concurrent workflow execution"
```

## Metrics Collected

### SimulationMetrics

- **duration_seconds**: Total simulation runtime
- **total_workflows**: Number of workflows executed
- **total_activities**: Number of activities executed
- **success_rate**: Overall success rate
- **scenario_results**: Per-scenario statistics
- **replay_errors**: Replay failures (should be 0)
- **nondeterministic_events**: Non-deterministic events detected (should be 0)
- **worker_restarts**: Number of worker restarts
- **activity_retries**: Total activity retry attempts
- **workflow_cancellations**: Workflows cancelled

### Per-Scenario Statistics

- **executed**: Total executions
- **succeeded**: Successful completions
- **failed**: Failures
- **timed_out**: Timeouts
- **cancelled**: Cancellations
- **avg_duration_ms**: Average execution time

## Architecture

```
SimulationOrchestrator
├── Phase 1: Spawn initial workflows
│   ├── ScenarioWorkflow (child_workflow)
│   ├── ScenarioWorkflow (signal_pattern)
│   ├── ScenarioWorkflow (activity_retry)
│   └── ... (other scenarios)
│
├── Phase 2: Monitor and manage
│   ├── Dynamic workflow spawning
│   ├── Random cancellation injection
│   └── Continuous monitoring
│
└── Phase 3: Collect metrics
    └── Aggregate and report results
```

## Scenario Types

### 1. Child Workflow Pattern
Tests nested workflow execution with configurable depth.

**Workflow**: `ChildWorkflowScenario`  
**Features**: Recursive child spawning, depth limiting, result aggregation

### 2. Signal Pattern
Tests signal delivery and state management.

**Workflow**: `SignalScenario`  
**Features**: Dynamic signal handling, state accumulation, completion signals

### 3. Activity Retry Pattern
Tests activity failures and retry policies.

**Activity**: `FlakeyActivity`  
**Features**: Controlled failure count, exponential backoff, retry limits

### 4. Timeout Pattern
Tests activity and workflow timeouts.

**Activity**: `TimeoutActivity`  
**Features**: Configurable sleep duration, timeout detection, error handling

### 5. Continue-as-New Pattern
Tests long-running workflow continuation.

**Workflow**: `ContinueAsNewScenario`  
**Features**: State preservation, iteration counting, clean continuation

### 6. Parallel Execution
Tests concurrent activity execution.

**Workflow**: `ParallelScenario`  
**Features**: Multiple activities, failure isolation, throughput testing

### 7. Complex Pattern
Combines multiple features for integration testing.

**Features**: Activities + Child workflows + Timers + Signals

## Determinism Guarantees

The simulation ensures determinism through:

1. **Controlled randomness**: All random decisions use workflow time
2. **Replay validation**: No replay errors tolerated
3. **Idempotent activities**: Activities handle retries correctly
4. **Side-effect isolation**: External state properly managed
5. **Metric validation**: Success rates within expected bounds

## Failure Injection

Failures are injected at multiple levels:

- **Activity failures**: Random failures based on `failure_rate`
- **Timeouts**: Activities exceed timeout based on `timeout_rate`
- **Cancellations**: Random workflow cancellation (5% probability)
- **Non-retryable errors**: Immediate failure propagation
- **Worker crashes**: Simulated via restart intervals

## Validation

Each simulation run validates:

1. ✅ **Zero replay errors**: All workflows replay correctly
2. ✅ **Zero non-deterministic events**: No non-determinism detected
3. ✅ **Expected success rate**: Results within configured bounds
4. ✅ **Metric consistency**: All metrics add up correctly
5. ✅ **Clean shutdown**: All workers shutdown gracefully

## Example Output

```
================================================================================
SIMULATION RESULTS
================================================================================
Duration: 60.5s
Workflows Executed: 24
Activities Executed: 120
Success Rate: 78.3%

Breakdown by Scenario:
  child_workflow:
    Executed: 4
    Succeeded: 3
    Failed: 1
    Timed Out: 0
    Cancelled: 0
  activity_retry:
    Executed: 5
    Succeeded: 4
    Failed: 1
    Timed Out: 0
    Cancelled: 0
  ...

Determinism Checks:
  Replay Errors: 0
  Non-deterministic Events: 0

Fault Tolerance:
  Worker Restarts: 4
  Activity Retries: 23
  Workflow Cancellations: 2
================================================================================
```

## Performance Testing

For performance testing, increase the configuration values:

```crystal
config = SimulationConfig.new(
  duration: 300.seconds,        # 5 minutes
  num_workflows: 100,           # 100 concurrent workflows
  activities_per_workflow: 10,  # 10 activities each
  failure_rate: 0.15,           # 15% failures
  timeout_rate: 0.05            # 5% timeouts
)
```

Expected throughput:
- **Workflows**: ~100-200 workflows/minute
- **Activities**: ~500-1000 activities/minute
- **Signals**: ~1000+ signals/minute

## Troubleshooting

### High Failure Rate
If failure rate exceeds expected bounds:
1. Check activity retry configuration
2. Verify timeout settings
3. Review worker resource limits

### Replay Errors
If replay errors occur (should be 0):
1. Check for non-deterministic code
2. Verify all random operations use workflow time
3. Review side effects in workflow code

### Low Throughput
If throughput is lower than expected:
1. Increase worker concurrency
2. Reduce sleep durations
3. Optimize activity execution time

## Contributing

When adding new scenarios:

1. Create workflow in `workflows/scenario_workflows.cr`
2. Create activities in `activities/simulation_activities.cr`
3. Add scenario type to `SimulationOrchestrator#select_scenario_type`
4. Update this README with scenario details
5. Add specific test in `simulation_spec.cr`

## License

Same as main Temporal.io Crystal SDK.
