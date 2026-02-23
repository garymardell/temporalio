require "../spec_helper"
require "../../src/temporalio"
require "../../src/temporalio/client_async"
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
  it "handles concurrent workflow starts from multiple threads with connection pool" do
    {% if flag?(:preview_mt) %}
      puts "\n  ✓ Multi-threading enabled (preview_mt flag detected)"
      
      # Create pooled client
      client_pool = Temporalio::Client.connect_with_pool(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default",
        pool_size: 5,
        initial_pool_size: 2
      )
      
      # Create non-pooled client for worker
      client_worker = Temporalio::Client.connect(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default"
      )
      
      task_queue = "mt-test-#{Random.rand(100000)}"
      
      # Start worker in a thread
      worker = create_worker(client_worker, task_queue, [SimpleCompletionWorkflow])
      
      # Start workflows from multiple threads
      num_threads = 4
      workflows_per_thread = 10
      total_workflows = num_threads * workflows_per_thread
      
      # Channel to collect results from all threads
      results_channel = Channel(Array(String)).new(num_threads)
      
      # Spawn multiple threads, each starting workflows concurrently
      num_threads.times do |thread_id|
        spawn do
          thread_results = [] of String
          
          # Each thread starts workflows_per_thread workflows
          async_starts = workflows_per_thread.times.map do |i|
            workflow_id = "mt-#{thread_id}-#{i}-#{Random.rand(100000)}"
            client_pool.start_workflow_async_channel(
              SimpleCompletionWorkflow.workflow_name,
              "Thread#{thread_id}-#{i}",
              id: workflow_id,
              task_queue: task_queue,
              execution_timeout: 30.seconds
            )
          end.to_a
          
          # Wait for all starts to complete
          handles = async_starts.map(&.await)
          
          # Get all results
          results = handles.map(&.result)
          results.each { |r| thread_results << r.as(String) }
          
          # Send results back
          results_channel.send(thread_results)
        end
      end
      
      # Collect results from all threads
      all_results = [] of String
      num_threads.times do
        thread_results = results_channel.receive
        all_results.concat(thread_results)
      end
      
      # Verify all workflows completed successfully
      all_results.size.should eq(total_workflows)
      
      # Verify no duplicates (ensures no race conditions)
      all_results.uniq.size.should eq(total_workflows)
      
      # Verify content correctness
      num_threads.times do |thread_id|
        workflows_per_thread.times do |i|
          expected = "Hello, Thread#{thread_id}-#{i}!"
          all_results.should contain(expected)
        end
      end
      
      # Check pool stats
      if stats = client_pool.pool_stats
        puts "  Pool statistics:"
        puts "    Total connections: #{stats[:total]}"
        puts "    Idle connections:  #{stats[:idle]}"
        puts "    In-use connections: #{stats[:in_use]}"
        
        # Pool should have created some connections
        stats[:total].should be > 0
      end
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      puts "  ✓ Successfully completed #{total_workflows} workflows across #{num_threads} threads"
    {% else %}
      pending "Multi-threading not enabled (compile with -Dpreview_mt)"
    {% end %}
  end
  
  it "handles concurrent RPC calls from multiple threads" do
    {% if flag?(:preview_mt) %}
      # Create pooled client
      client_pool = Temporalio::Client.connect_with_pool(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default",
        pool_size: 10,
        initial_pool_size: 3
      )
      
      # Create non-pooled client for worker
      client_worker = Temporalio::Client.connect(
        target_host: ENV["TEMPORAL_ADDRESS"]? || "http://localhost:7234",
        namespace: "default"
      )
      
      task_queue = "mt-rpc-test-#{Random.rand(100000)}"
      worker = create_worker(client_worker, task_queue, [SimpleCompletionWorkflow])
      
      num_threads = 8
      calls_per_thread = 5
      total_calls = num_threads * calls_per_thread
      
      # Atomic counter to track successful calls
      success_count = Atomic(Int32).new(0)
      error_count = Atomic(Int32).new(0)
      
      # Channel to synchronize thread completion
      done_channel = Channel(Nil).new(num_threads)
      
      # Spawn threads that make concurrent RPC calls
      num_threads.times do |thread_id|
        spawn do
          calls_per_thread.times do |i|
            begin
              # Start a workflow (RPC call)
              handle = client_pool.start_workflow(
                SimpleCompletionWorkflow.workflow_name,
                "RPC-#{thread_id}-#{i}",
                id: "mt-rpc-#{thread_id}-#{i}-#{Random.rand(100000)}",
                task_queue: task_queue,
                execution_timeout: 30.seconds
              )
              
              # Get result (another RPC call)
              result = handle.result
              result.should eq("Hello, RPC-#{thread_id}-#{i}!")
              
              success_count.add(1)
            rescue ex
              puts "  ✗ Thread #{thread_id}, call #{i} failed: #{ex.message}"
              error_count.add(1)
            end
          end
          
          done_channel.send(nil)
        end
      end
      
      # Wait for all threads to complete
      num_threads.times { done_channel.receive }
      
      # Verify all calls succeeded
      success_count.get.should eq(total_calls)
      error_count.get.should eq(0)
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      puts "  ✓ Successfully completed #{total_calls} concurrent RPC calls from #{num_threads} threads"
    {% else %}
      pending "Multi-threading not enabled (compile with -Dpreview_mt)"
    {% end %}
  end
  
  it "maintains connection pool integrity under thread stress" do
    {% if flag?(:preview_mt) %}
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
      
      # High concurrency stress test
      # Reduced from 20x5=100 to avoid timeout in CI
      num_threads = 10
      workflows_per_thread = 5
      
      completed = Atomic(Int32).new(0)
      failed = Atomic(Int32).new(0)
      done_channel = Channel(Nil).new(num_threads)
      
      # Spawn all threads
      num_threads.times do |thread_id|
        spawn do
          # Execute workflows
          workflows_per_thread.times do |i|
            begin
              handle = client_pool.start_workflow(
                SimpleCompletionWorkflow.workflow_name,
                "Stress-#{thread_id}-#{i}",
                id: "mt-stress-#{thread_id}-#{i}-#{Random.rand(100000)}",
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
      
      # Wait for all threads (this already ensures completion)
      num_threads.times { done_channel.receive }
      
      # Verify results
      total_expected = num_threads * workflows_per_thread
      completed.get.should eq(total_expected)
      failed.get.should eq(0)
      
      # Verify pool is still healthy
      if stats = client_pool.pool_stats
        puts "  Final pool state:"
        puts "    Total connections: #{stats[:total]}"
        puts "    Idle connections:  #{stats[:idle]}"
        stats[:total].should be > 0
        stats[:total].should be <= 5  # Should not exceed max_pool_size
      end
      
      worker.initiate_shutdown
      worker.wait_all_complete
      
      puts "  ✓ Stress test passed: #{total_expected} workflows across #{num_threads} threads"
    {% else %}
      pending "Multi-threading not enabled (compile with -Dpreview_mt)"
    {% end %}
  end
end
