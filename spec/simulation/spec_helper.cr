require "../spec_helper"
require "../../src/temporalio"
require "./simulation_config"
require "./workflows/simulation_orchestrator"
require "./workflows/scenario_workflows"
require "./activities/simulation_activities"

def simulation_create_client
  Temporalio::Client.connect(
    target_host: "http://localhost:7234",
    namespace: "default"
  )
end

def simulation_create_worker(client, task_queue, workflows, activities)
  Temporalio::Worker.new(
    client: client,
    task_queue: task_queue,
    workflows: workflows,
    activities: activities
  )
end
