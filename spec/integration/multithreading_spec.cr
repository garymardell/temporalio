require "../spec_helper"
require "../../src/temporalio"
require "./workflows/simple_completion_workflow"

# Helper to create worker
private macro create_worker(client, task_queue, workflows)
  %workflow_defs = Array(Temporalio::Internal::WorkflowDefinition).new
  {% for wf in workflows %}
    %workflow_defs << Temporalio::Internal::ConcreteWorkflowDefinition({{wf}}).new
  {% end %}

  %worker = Temporalio::Worker.new(
    client: {{client}},
    task_queue: {{task_queue}},
    workflows: %workflow_defs,
    activities: [] of Temporalio::Internal::ActivityDefinition
  )

  spawn { %worker.run }
  sleep 200.milliseconds
  %worker
end

# Test connection pooling with multi-threading enabled
# This ensures thread safety of the connection pool and underlying clients
#
# Run with: crystal spec spec/integration/multithreading_spec.cr -Dpreview_mt

describe "Multi-threading safety" do
  {% if flag?(:preview_mt) %}
    it "handles concurrent workflow starts from multiple fibers with connection pool" do
      puts "\n  ✓ Multi-threading enabled (preview_mt flag detected)"

      client_pool = Temporalio::Client.connect_with_pool(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default",
        pool_size: 5,
        initial_pool_size: 2
      )

      client_worker = Temporalio::Client.connect(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default"
      )

      task_queue = "mt-test-#{Random.rand(100000)}"
      worker = create_worker(client_worker, task_queue, [SimpleCompletionWorkflow])

      num_fibers = 4
      workflows_per_fiber = 10
      total_workflows = num_fibers * workflows_per_fiber

      results_channel = Channel(Array(String)).new(num_fibers)

      num_fibers.times do |fiber_id|
        spawn do
          fiber_results = [] of String

          workflows_per_fiber.times do |i|
            workflow_id = "mt-#{fiber_id}-#{i}-#{Random.rand(100000)}"
            handle = client_pool.start_workflow(
              SimpleCompletionWorkflow.workflow_name,
              "Thread#{fiber_id}-#{i}",
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
            result = handle.result
            fiber_results << result.as(String)
          end

          results_channel.send(fiber_results)
        end
      end

      all_results = [] of String
      num_fibers.times do
        all_results.concat(results_channel.receive)
      end

      all_results.size.should eq(total_workflows)
      all_results.uniq.size.should eq(total_workflows)

      num_fibers.times do |fiber_id|
        workflows_per_fiber.times do |i|
          all_results.should contain("Hello, Thread#{fiber_id}-#{i}!")
        end
      end

      if stats = client_pool.pool_stats
        stats[:total].should be > 0
      end

      worker.initiate_shutdown
      worker.wait_all_complete

      puts "  ✓ Successfully completed #{total_workflows} workflows across #{num_fibers} fibers"
    end

    it "handles concurrent RPC calls from multiple fibers" do
      client_pool = Temporalio::Client.connect_with_pool(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default",
        pool_size: 10,
        initial_pool_size: 3
      )

      client_worker = Temporalio::Client.connect(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default"
      )

      task_queue = "mt-rpc-test-#{Random.rand(100000)}"
      worker = create_worker(client_worker, task_queue, [SimpleCompletionWorkflow])

      num_fibers = 8
      calls_per_fiber = 5
      total_calls = num_fibers * calls_per_fiber

      success_count = Atomic(Int32).new(0)
      error_count = Atomic(Int32).new(0)
      done_channel = Channel(Nil).new(num_fibers)

      num_fibers.times do |fiber_id|
        spawn do
          calls_per_fiber.times do |i|
            begin
              handle = client_pool.start_workflow(
                SimpleCompletionWorkflow.workflow_name,
                "RPC-#{fiber_id}-#{i}",
                id: "mt-rpc-#{fiber_id}-#{i}-#{Random.rand(100000)}",
                task_queue: task_queue,
                execution_timeout: 30.seconds
              )

              result = handle.result
              result.should eq("Hello, RPC-#{fiber_id}-#{i}!")

              success_count.add(1)
            rescue ex
              puts "  ✗ Fiber #{fiber_id}, call #{i} failed: #{ex.message}"
              error_count.add(1)
            end
          end

          done_channel.send(nil)
        end
      end

      num_fibers.times { done_channel.receive }

      success_count.get.should eq(total_calls)
      error_count.get.should eq(0)

      worker.initiate_shutdown
      worker.wait_all_complete

      puts "  ✓ Successfully completed #{total_calls} concurrent RPC calls from #{num_fibers} fibers"
    end

    it "maintains connection pool integrity under stress" do
      client_pool = Temporalio::Client.connect_with_pool(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default",
        pool_size: 5,
        initial_pool_size: 1
      )

      client_worker = Temporalio::Client.connect(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default"
      )

      task_queue = "mt-stress-#{Random.rand(100000)}"
      worker = create_worker(client_worker, task_queue, [SimpleCompletionWorkflow])

      num_fibers = 10
      workflows_per_fiber = 5

      completed = Atomic(Int32).new(0)
      failed = Atomic(Int32).new(0)
      done_channel = Channel(Nil).new(num_fibers)

      num_fibers.times do |fiber_id|
        spawn do
          workflows_per_fiber.times do |i|
            begin
              handle = client_pool.start_workflow(
                SimpleCompletionWorkflow.workflow_name,
                "Stress-#{fiber_id}-#{i}",
                id: "mt-stress-#{fiber_id}-#{i}-#{Random.rand(100000)}",
                task_queue: task_queue,
                execution_timeout: 30.seconds
              )
              handle.result
              completed.add(1)
            rescue ex
              puts "  ✗ Stress test failure: #{ex.message}"
              failed.add(1)
            end
          end
          done_channel.send(nil)
        end
      end

      num_fibers.times { done_channel.receive }

      total_expected = num_fibers * workflows_per_fiber
      completed.get.should eq(total_expected)
      failed.get.should eq(0)

      if stats = client_pool.pool_stats
        stats[:total].should be > 0
        stats[:total].should be <= 5
      end

      worker.initiate_shutdown
      worker.wait_all_complete

      puts "  ✓ Stress test passed: #{total_expected} workflows across #{num_fibers} fibers"
    end
  {% else %}
    pending "handles concurrent workflow starts from multiple fibers with connection pool (compile with -Dpreview_mt)"
    pending "handles concurrent RPC calls from multiple fibers (compile with -Dpreview_mt)"
    pending "maintains connection pool integrity under stress (compile with -Dpreview_mt)"
  {% end %}
end
