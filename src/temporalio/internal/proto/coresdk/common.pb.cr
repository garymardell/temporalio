require "protobuf"

# Hand-written Crystal proto bindings for coresdk.common

module Coresdk
  module Common
    enum VersioningIntent
      UNSPECIFIED = 0
      COMPATIBLE  = 1
      DEFAULT     = 2
    end

    struct NamespacedWorkflowExecution
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :namespace, :string, 1
        optional :workflow_id, :string, 2
        optional :run_id, :string, 3
      end
    end

    struct WorkerDeploymentVersion
      include ::Protobuf::Message
      contract_of "proto3" do
        optional :deployment_name, :string, 1
        optional :build_id, :string, 2
      end
    end
  end
end
