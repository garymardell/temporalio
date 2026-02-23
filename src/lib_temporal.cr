{% if flag?(:darwin) %}
  @[Link(ldflags: "#{__DIR__}/../ext/sdk-core/target/release/libtemporalio_sdk_core_c_bridge.dylib")]
{% else %}
  @[Link(ldflags: "#{__DIR__}/../ext/sdk-core/target/release/libtemporalio_sdk_core_c_bridge.so")]
{% end %}
lib TemporalCore
  alias CBool = LibC::UInt8T
  alias CString = LibC::Char*

  # -------------------- Enums --------------------

  enum TemporalCoreRpcService : Int32
    Workflow = 1
    Operator
    Cloud
    Test
    Health
  end

  enum TemporalCoreMetricAttributeValueType : Int32
    String = 1
    Int
    Float
    Bool
  end

  enum TemporalCoreMetricKind : Int32
    CounterInteger = 1
    HistogramInteger
    HistogramFloat
    HistogramDuration
    GaugeInteger
    GaugeFloat
  end

  enum TemporalCoreForwardedLogLevel : Int32
    Trace = 0
    Debug
    Info
    Warn
    Error
  end

  enum TemporalCoreOpenTelemetryMetricTemporality : Int32
    Cumulative = 1
    Delta
  end

  enum TemporalCoreOpenTelemetryProtocol : Int32
    Grpc = 1
    Http
  end

  enum TemporalCoreSlotKindType : Int32
    WorkflowSlotKindType
    ActivitySlotKindType
    LocalActivitySlotKindType
    NexusSlotKindType
  end

  enum TemporalCoreWorkerVersioningStrategy_Tag : Int32
    None
    DeploymentBased
    LegacyBuildIdBased
  end

  enum TemporalCoreSlotInfo_Tag : Int32
    WorkflowSlotInfo
    ActivitySlotInfo
    LocalActivitySlotInfo
    NexusSlotInfo
  end

  enum TemporalCoreSlotSupplier_Tag : Int32
    FixedSize
    ResourceBased
    Custom
  end

  # -------------------- Opaque types --------------------
  alias TemporalCoreCancellationToken = UInt8*
  alias TemporalCoreClient = UInt8*
  alias TemporalCoreClientGrpcOverrideRequest = UInt8*
  alias TemporalCoreEphemeralServer = UInt8*
  alias TemporalCoreForwardedLog = UInt8*
  alias TemporalCoreMetric = UInt8*
  alias TemporalCoreMetricAttributes = UInt8*
  alias TemporalCoreMetricMeter = UInt8*
  alias TemporalCoreRandom = UInt8*
  alias TemporalCoreRuntime = UInt8*
  alias TemporalCoreSlotReserveCompletionCtx = UInt8*
  alias TemporalCoreWorker = UInt8*
  alias TemporalCoreWorkerReplayPusher = UInt8*
  # alias TemporalCoreCustomMetricMeter = UInt8*

  # -------------------- Basic structs --------------------

  struct TemporalCoreByteArrayRef
    data : UInt8*
    size : LibC::SizeT
  end

  # Metadata uses the same representation as ByteArrayRef
  alias TemporalCoreMetadataRef = TemporalCoreByteArrayRef

  struct TemporalCoreClientTlsOptions
    server_root_ca_cert : TemporalCoreByteArrayRef
    domain : TemporalCoreByteArrayRef
    client_cert : TemporalCoreByteArrayRef
    client_private_key : TemporalCoreByteArrayRef
  end

  struct TemporalCoreClientRetryOptions
    initial_interval_millis : UInt64
    randomization_factor : Float64
    multiplier : Float64
    max_interval_millis : UInt64
    max_elapsed_time_millis : UInt64
    max_retries : LibC::UInt8T
  end

  struct TemporalCoreClientKeepAliveOptions
    interval_millis : UInt64
    timeout_millis : UInt64
  end

  struct TemporalCoreClientHttpConnectProxyOptions
    target_host : TemporalCoreByteArrayRef
    username : TemporalCoreByteArrayRef
    password : TemporalCoreByteArrayRef
  end

  # Callback invoked for every gRPC call if set
  type TemporalCoreClientGrpcOverrideCallback = (TemporalCoreClientGrpcOverrideRequest*, Void*) -> Void

  struct TemporalCoreClientOptions
    target_url : TemporalCoreByteArrayRef
    client_name : TemporalCoreByteArrayRef
    client_version : TemporalCoreByteArrayRef
    metadata : TemporalCoreMetadataRef
    api_key : TemporalCoreByteArrayRef
    identity : TemporalCoreByteArrayRef
    tls_options : TemporalCoreClientTlsOptions*
    retry_options : TemporalCoreClientRetryOptions*
    keep_alive_options : TemporalCoreClientKeepAliveOptions*
    http_connect_proxy_options : TemporalCoreClientHttpConnectProxyOptions*
    grpc_override_callback : TemporalCoreClientGrpcOverrideCallback
    grpc_override_callback_user_data : Void*
  end

  struct TemporalCoreByteArray
    data : UInt8*
    size : LibC::SizeT
    cap : LibC::SizeT          # internal use
    disable_free : CBool       # internal use
  end

  # Connect callback: success client or fail (ByteArray)
  alias TemporalCoreClientConnectCallback = (Void*, TemporalCoreClient*, TemporalCoreByteArray*) -> Nil

  struct TemporalCoreClientGrpcOverrideResponse
    status_code : Int32
    headers : TemporalCoreMetadataRef
    success_proto : TemporalCoreByteArrayRef
    fail_message : TemporalCoreByteArrayRef
    fail_details : TemporalCoreByteArrayRef
  end

  struct TemporalCoreRpcCallOptions
    service : TemporalCoreRpcService
    rpc : TemporalCoreByteArrayRef
    req : TemporalCoreByteArrayRef
    retry : CBool
    metadata : TemporalCoreMetadataRef
    timeout_millis : UInt32     # 0 means no timeout
    cancellation_token : TemporalCoreCancellationToken*
  end

  # RPC call callback
  alias TemporalCoreClientRpcCallCallback = (Void*, TemporalCoreByteArray*, UInt32, TemporalCoreByteArray*, TemporalCoreByteArray*) -> Nil

  struct TemporalCoreClientEnvConfigOrFail
    success : TemporalCoreByteArray*
    fail : TemporalCoreByteArray*
  end

  struct TemporalCoreClientEnvConfigLoadOptions
    path : TemporalCoreByteArrayRef
    data : TemporalCoreByteArrayRef
    config_file_strict : CBool
    env_vars : TemporalCoreByteArrayRef
  end

  struct TemporalCoreClientEnvConfigProfileOrFail
    success : TemporalCoreByteArray*
    fail : TemporalCoreByteArray*
  end

  struct TemporalCoreClientEnvConfigProfileLoadOptions
    profile : TemporalCoreByteArrayRef
    path : TemporalCoreByteArrayRef
    data : TemporalCoreByteArrayRef
    disable_file : CBool
    disable_env : CBool
    config_file_strict : CBool
    env_vars : TemporalCoreByteArrayRef
  end

  union TemporalCoreMetricAttributeValue
    string_value : TemporalCoreByteArrayRef
    int_value : Int64
    float_value : Float64
    bool_value : CBool
  end

  struct TemporalCoreMetricAttribute
    key : TemporalCoreByteArrayRef
    value : TemporalCoreMetricAttributeValue
    value_type : TemporalCoreMetricAttributeValueType
  end

  struct TemporalCoreMetricOptions
    name : TemporalCoreByteArrayRef
    description : TemporalCoreByteArrayRef
    unit : TemporalCoreByteArrayRef
    kind : TemporalCoreMetricKind
  end

  # Forwarded log callback
  type TemporalCoreForwardedLogCallback = (TemporalCoreForwardedLogLevel, TemporalCoreForwardedLog*) -> Void

  struct TemporalCoreLoggingOptions
    filter : TemporalCoreByteArrayRef
    forward_to : TemporalCoreForwardedLogCallback
  end

  struct TemporalCoreOpenTelemetryOptions
    url : TemporalCoreByteArrayRef
    headers : TemporalCoreMetadataRef
    metric_periodicity_millis : UInt32
    metric_temporality : TemporalCoreOpenTelemetryMetricTemporality
    durations_as_seconds : CBool
    protocol : TemporalCoreOpenTelemetryProtocol
    histogram_bucket_overrides : TemporalCoreMetadataRef
  end

  struct TemporalCorePrometheusOptions
    bind_address : TemporalCoreByteArrayRef
    counters_total_suffix : CBool
    unit_suffix : CBool
    durations_as_seconds : CBool
    histogram_bucket_overrides : TemporalCoreMetadataRef
  end

  # ----- Custom metrics (user-implemented meter) -----

  struct TemporalCoreCustomMetricAttributeValueString
    data : UInt8*
    size : LibC::SizeT
  end

  union TemporalCoreCustomMetricAttributeValue
    string_value : TemporalCoreCustomMetricAttributeValueString
    int_value : Int64
    float_value : Float64
    bool_value : CBool
  end

  struct TemporalCoreCustomMetricAttribute
    key : TemporalCoreByteArrayRef
    value : TemporalCoreCustomMetricAttributeValue
    value_type : TemporalCoreMetricAttributeValueType
  end


  struct TemporalCoreCustomMetricMeter
    metric_new : TemporalCoreCustomMetricMeterMetricNewCallback
    metric_free : TemporalCoreCustomMetricMeterMetricFreeCallback
    metric_record_integer : TemporalCoreCustomMetricMeterMetricRecordIntegerCallback
    metric_record_float : TemporalCoreCustomMetricMeterMetricRecordFloatCallback
    metric_record_duration : TemporalCoreCustomMetricMeterMetricRecordDurationCallback
    attributes_new : TemporalCoreCustomMetricMeterAttributesNewCallback
    attributes_free : TemporalCoreCustomMetricMeterAttributesFreeCallback
    meter_free : TemporalCoreCustomMetricMeterMeterFreeCallback
  end

  type TemporalCoreCustomMetricMeterMetricNewCallback = (TemporalCoreByteArrayRef, TemporalCoreByteArrayRef, TemporalCoreByteArrayRef, TemporalCoreMetricKind) -> Void*
  type TemporalCoreCustomMetricMeterMetricFreeCallback = (Void*) -> Void
  type TemporalCoreCustomMetricMeterMetricRecordIntegerCallback = (Void*, UInt64, Void*) -> Void
  type TemporalCoreCustomMetricMeterMetricRecordFloatCallback = (Void*, Float64, Void*) -> Void
  type TemporalCoreCustomMetricMeterMetricRecordDurationCallback = (Void*, UInt64, Void*) -> Void
  type TemporalCoreCustomMetricMeterAttributesNewCallback = (Void*, TemporalCoreCustomMetricAttribute*, LibC::SizeT) -> Void*
  type TemporalCoreCustomMetricMeterAttributesFreeCallback = (Void*) -> Void
  type TemporalCoreCustomMetricMeterMeterFreeCallback = (TemporalCoreCustomMetricMeter*) -> Void

  struct TemporalCoreMetricsOptions
    opentelemetry : TemporalCoreOpenTelemetryOptions*
    prometheus : TemporalCorePrometheusOptions*
    custom_meter : TemporalCoreCustomMetricMeter*
    attach_service_name : CBool
    global_tags : TemporalCoreMetadataRef
    metric_prefix : TemporalCoreByteArrayRef
  end

  struct TemporalCoreTelemetryOptions
    logging : TemporalCoreLoggingOptions*
    metrics : TemporalCoreMetricsOptions*
  end

  struct TemporalCoreRuntimeOptions
    telemetry : TemporalCoreTelemetryOptions*
    worker_heartbeat_duration_millis : UInt64
  end

  struct TemporalCoreRuntimeOrFail
    runtime : TemporalCoreRuntime*
    fail : TemporalCoreByteArray*
  end

  struct TemporalCoreTestServerOptions
    existing_path : TemporalCoreByteArrayRef
    sdk_name : TemporalCoreByteArrayRef
    sdk_version : TemporalCoreByteArrayRef
    download_version : TemporalCoreByteArrayRef
    download_dest_dir : TemporalCoreByteArrayRef
    port : UInt16
    extra_args : TemporalCoreByteArrayRef
    download_ttl_seconds : UInt64
  end

  struct TemporalCoreDevServerOptions
    test_server : TemporalCoreTestServerOptions*     # must always be present
    namespace_ : TemporalCoreByteArrayRef
    ip : TemporalCoreByteArrayRef
    database_filename : TemporalCoreByteArrayRef
    ui : CBool
    ui_port : UInt16
    log_format : TemporalCoreByteArrayRef
    log_level : TemporalCoreByteArrayRef
  end

  # Ephemeral server callbacks
  type TemporalCoreEphemeralServerStartCallback = (Void*, TemporalCoreEphemeralServer*, TemporalCoreByteArray*, TemporalCoreByteArray*) -> Void
  type TemporalCoreEphemeralServerShutdownCallback = (Void*, TemporalCoreByteArray*) -> Void

  # Worker OrFail / Replay structs
  struct TemporalCoreWorkerOrFail
    worker : TemporalCoreWorker*
    fail : TemporalCoreByteArray*
  end

  struct TemporalCoreWorkerVersioningNone
    build_id : TemporalCoreByteArrayRef
  end

  struct TemporalCoreWorkerDeploymentVersion
    deployment_name : TemporalCoreByteArrayRef
    build_id : TemporalCoreByteArrayRef
  end

  struct TemporalCoreWorkerDeploymentOptions
    version : TemporalCoreWorkerDeploymentVersion
    use_worker_versioning : CBool
    default_versioning_behavior : Int32
  end

  struct TemporalCoreLegacyBuildIdBasedStrategy
    build_id : TemporalCoreByteArrayRef
  end

  struct TemporalCoreWorkerVersioningStrategy
    tag : TemporalCoreWorkerVersioningStrategy_Tag
  #   union
  #     none : TemporalCoreWorkerVersioningNone
  #     deployment_based : TemporalCoreWorkerDeploymentOptions
  #     legacy_build_id_based : TemporalCoreLegacyBuildIdBasedStrategy
  #   end
  end

  struct TemporalCoreFixedSizeSlotSupplier
    num_slots : LibC::UInt8T
  end

  struct TemporalCoreResourceBasedTunerOptions
    target_memory_usage : Float64
    target_cpu_usage : Float64
  end

  struct TemporalCoreResourceBasedSlotSupplier
    minimum_slots : LibC::UInt8T
    maximum_slots : LibC::UInt8T
    ramp_throttle_ms : UInt64
    tuner_options : TemporalCoreResourceBasedTunerOptions
  end

  struct TemporalCoreSlotReserveCtx
    slot_type : TemporalCoreSlotKindType
    task_queue : TemporalCoreByteArrayRef
    worker_identity : TemporalCoreByteArrayRef
    worker_build_id : TemporalCoreByteArrayRef
    is_sticky : CBool
  end

  type TemporalCoreCustomSlotSupplierReserveCallback = (TemporalCoreSlotReserveCtx*, TemporalCoreSlotReserveCompletionCtx*, Void*) -> Void
  type TemporalCoreCustomSlotSupplierCancelReserveCallback = (TemporalCoreSlotReserveCompletionCtx*, Void*) -> Void
  type TemporalCoreCustomSlotSupplierTryReserveCallback = (TemporalCoreSlotReserveCtx*, Void*) -> LibC::UInt8T

  struct TemporalCoreWorkflowSlotInfo_Body
    workflow_type : TemporalCoreByteArrayRef
    is_sticky : CBool
  end

  struct TemporalCoreActivitySlotInfo_Body
    activity_type : TemporalCoreByteArrayRef
  end

  struct TemporalCoreLocalActivitySlotInfo_Body
    activity_type : TemporalCoreByteArrayRef
  end

  struct TemporalCoreNexusSlotInfo_Body
    operation : TemporalCoreByteArrayRef
    service : TemporalCoreByteArrayRef
  end

  struct TemporalCoreSlotInfo
    tag : TemporalCoreSlotInfo_Tag
    # union
    #   workflow_slot_info : TemporalCoreWorkflowSlotInfo_Body
    #   activity_slot_info : TemporalCoreActivitySlotInfo_Body
    #   local_activity_slot_info : TemporalCoreLocalActivitySlotInfo_Body
    #   nexus_slot_info : TemporalCoreNexusSlotInfo_Body
    # end
  end

  struct TemporalCoreSlotMarkUsedCtx
    slot_info : TemporalCoreSlotInfo
    slot_permit : LibC::UInt8T
  end

  type TemporalCoreCustomSlotSupplierMarkUsedCallback = (TemporalCoreSlotMarkUsedCtx*, Void*) -> Void

  struct TemporalCoreSlotReleaseCtx
    slot_info : TemporalCoreSlotInfo*
    slot_permit : LibC::UInt8T
  end


  struct TemporalCoreCustomSlotSupplierCallbacks
    reserve : TemporalCoreCustomSlotSupplierReserveCallback
    cancel_reserve : TemporalCoreCustomSlotSupplierCancelReserveCallback
    try_reserve : TemporalCoreCustomSlotSupplierTryReserveCallback
    mark_used : TemporalCoreCustomSlotSupplierMarkUsedCallback
    release : TemporalCoreCustomSlotSupplierReleaseCallback
    available_slots : TemporalCoreCustomSlotSupplierAvailableSlotsCallback
    free : TemporalCoreCustomSlotSupplierFreeCallback
    user_data : Void*
  end

  type TemporalCoreCustomSlotSupplierReleaseCallback = (TemporalCoreSlotReleaseCtx*, Void*) -> Void
  type TemporalCoreCustomSlotSupplierAvailableSlotsCallback = (LibC::UInt8T*, Void*) -> CBool
  type TemporalCoreCustomSlotSupplierFreeCallback = (TemporalCoreCustomSlotSupplierCallbacks*) -> Void

  struct TemporalCoreCustomSlotSupplierCallbacksImpl
    _0 : TemporalCoreCustomSlotSupplierCallbacks*
  end

  struct TemporalCoreSlotSupplier
    tag : TemporalCoreSlotSupplier_Tag
    # union
    #   fixed_size : TemporalCoreFixedSizeSlotSupplier
    #   resource_based : TemporalCoreResourceBasedSlotSupplier
    #   custom : TemporalCoreCustomSlotSupplierCallbacksImpl
    # end
  end

  struct TemporalCoreTunerHolder
    workflow_slot_supplier : TemporalCoreSlotSupplier
    activity_slot_supplier : TemporalCoreSlotSupplier
    local_activity_slot_supplier : TemporalCoreSlotSupplier
    nexus_task_slot_supplier : TemporalCoreSlotSupplier
  end

  struct TemporalCorePollerBehaviorSimpleMaximum
    simple_maximum : LibC::UInt8T
  end

  struct TemporalCorePollerBehaviorAutoscaling
    minimum : LibC::UInt8T
    maximum : LibC::UInt8T
    initial : LibC::UInt8T
  end

  struct TemporalCorePollerBehavior
    simple_maximum : TemporalCorePollerBehaviorSimpleMaximum*
    autoscaling : TemporalCorePollerBehaviorAutoscaling*
  end

  struct TemporalCoreByteArrayRefArray
    data : TemporalCoreByteArrayRef*
    size : LibC::SizeT
  end

  struct TemporalCoreWorkerOptions
    namespace_ : TemporalCoreByteArrayRef
    task_queue : TemporalCoreByteArrayRef
    versioning_strategy : TemporalCoreWorkerVersioningStrategy
    identity_override : TemporalCoreByteArrayRef
    max_cached_workflows : UInt32
    tuner : TemporalCoreTunerHolder
    no_remote_activities : CBool
    sticky_queue_schedule_to_start_timeout_millis : UInt64
    max_heartbeat_throttle_interval_millis : UInt64
    default_heartbeat_throttle_interval_millis : UInt64
    max_activities_per_second : Float64
    max_task_queue_activities_per_second : Float64
    graceful_shutdown_period_millis : UInt64
    workflow_task_poller_behavior : TemporalCorePollerBehavior
    nonsticky_to_sticky_poll_ratio : Float32
    activity_task_poller_behavior : TemporalCorePollerBehavior
    nexus_task_poller_behavior : TemporalCorePollerBehavior
    nondeterminism_as_workflow_fail : CBool
    nondeterminism_as_workflow_fail_for_types : TemporalCoreByteArrayRefArray
  end

  # Worker callbacks
  alias TemporalCoreWorkerCallback = (Void*, TemporalCoreByteArray*) -> Nil
  alias TemporalCoreWorkerPollCallback = (Void*, TemporalCoreByteArray*, TemporalCoreByteArray*) -> Nil

  struct TemporalCoreWorkerReplayerOrFail
    worker : TemporalCoreWorker*
    worker_replay_pusher : TemporalCoreWorkerReplayPusher*
    fail : TemporalCoreByteArray*
  end

  struct TemporalCoreWorkerReplayPushResult
    fail : TemporalCoreByteArray*
  end

  # -------------------- Functions --------------------

  fun temporal_core_cancellation_token_new : TemporalCoreCancellationToken*
  fun temporal_core_cancellation_token_cancel(token : TemporalCoreCancellationToken*) : Void
  fun temporal_core_cancellation_token_free(token : TemporalCoreCancellationToken*) : Void

  fun temporal_core_client_connect(runtime : TemporalCoreRuntime*, options : TemporalCoreClientOptions*, user_data : Void*, callback : TemporalCoreClientConnectCallback) : Void
  fun temporal_core_client_free(client : TemporalCoreClient*) : Void
  fun temporal_core_client_update_metadata(client : TemporalCoreClient*, metadata : TemporalCoreByteArrayRef) : Void
  fun temporal_core_client_update_api_key(client : TemporalCoreClient*, api_key : TemporalCoreByteArrayRef) : Void

  fun temporal_core_client_grpc_override_request_service(req : TemporalCoreClientGrpcOverrideRequest*) : TemporalCoreByteArrayRef
  fun temporal_core_client_grpc_override_request_rpc(req : TemporalCoreClientGrpcOverrideRequest*) : TemporalCoreByteArrayRef
  fun temporal_core_client_grpc_override_request_headers(req : TemporalCoreClientGrpcOverrideRequest*) : TemporalCoreMetadataRef
  fun temporal_core_client_grpc_override_request_proto(req : TemporalCoreClientGrpcOverrideRequest*) : TemporalCoreByteArrayRef
  fun temporal_core_client_grpc_override_request_respond(req : TemporalCoreClientGrpcOverrideRequest*, resp : TemporalCoreClientGrpcOverrideResponse) : Void

  fun temporal_core_client_rpc_call(client : TemporalCoreClient*, options : TemporalCoreRpcCallOptions*, user_data : Void*, callback : TemporalCoreClientRpcCallCallback) : Void

  fun temporal_core_client_env_config_load(options : TemporalCoreClientEnvConfigLoadOptions*) : TemporalCoreClientEnvConfigOrFail
  fun temporal_core_client_env_config_profile_load(options : TemporalCoreClientEnvConfigProfileLoadOptions*) : TemporalCoreClientEnvConfigProfileOrFail

  fun temporal_core_metric_meter_new(runtime : TemporalCoreRuntime*) : TemporalCoreMetricMeter*
  fun temporal_core_metric_meter_free(meter : TemporalCoreMetricMeter*) : Void
  fun temporal_core_metric_attributes_new(meter : TemporalCoreMetricMeter*, attrs : TemporalCoreMetricAttribute*, size : LibC::SizeT) : TemporalCoreMetricAttributes*
  fun temporal_core_metric_attributes_new_append(meter : TemporalCoreMetricMeter*, orig : TemporalCoreMetricAttributes*, attrs : TemporalCoreMetricAttribute*, size : LibC::SizeT) : TemporalCoreMetricAttributes*
  fun temporal_core_metric_attributes_free(attrs : TemporalCoreMetricAttributes*) : Void
  fun temporal_core_metric_new(meter : TemporalCoreMetricMeter*, options : TemporalCoreMetricOptions*) : TemporalCoreMetric*
  fun temporal_core_metric_free(metric : TemporalCoreMetric*) : Void
  fun temporal_core_metric_record_integer(metric : TemporalCoreMetric*, value : UInt64, attrs : TemporalCoreMetricAttributes*) : Void
  fun temporal_core_metric_record_float(metric : TemporalCoreMetric*, value : Float64, attrs : TemporalCoreMetricAttributes*) : Void
  fun temporal_core_metric_record_duration(metric : TemporalCoreMetric*, value_ms : UInt64, attrs : TemporalCoreMetricAttributes*) : Void

  fun temporal_core_random_new(seed : UInt64) : TemporalCoreRandom*
  fun temporal_core_random_free(random : TemporalCoreRandom*) : Void
  fun temporal_core_random_int32_range(random : TemporalCoreRandom*, min : Int32, max : Int32, max_inclusive : CBool) : Int32
  fun temporal_core_random_double_range(random : TemporalCoreRandom*, min : Float64, max : Float64, max_inclusive : CBool) : Float64
  fun temporal_core_random_fill_bytes(random : TemporalCoreRandom*, bytes : TemporalCoreByteArrayRef) : Void

  fun temporal_core_runtime_new(options : TemporalCoreRuntimeOptions*) : TemporalCoreRuntimeOrFail
  fun temporal_core_runtime_free(runtime : TemporalCoreRuntime*) : Void
  fun temporal_core_byte_array_free(runtime : TemporalCoreRuntime*, bytes : TemporalCoreByteArray*) : Void

  fun temporal_core_forwarded_log_target(log : TemporalCoreForwardedLog*) : TemporalCoreByteArrayRef
  fun temporal_core_forwarded_log_message(log : TemporalCoreForwardedLog*) : TemporalCoreByteArrayRef
  fun temporal_core_forwarded_log_timestamp_millis(log : TemporalCoreForwardedLog*) : UInt64
  fun temporal_core_forwarded_log_fields_json(log : TemporalCoreForwardedLog*) : TemporalCoreByteArrayRef

  fun temporal_core_ephemeral_server_start_dev_server(runtime : TemporalCoreRuntime*, options : TemporalCoreDevServerOptions*, user_data : Void*, callback : TemporalCoreEphemeralServerStartCallback) : Void
  fun temporal_core_ephemeral_server_start_test_server(runtime : TemporalCoreRuntime*, options : TemporalCoreTestServerOptions*, user_data : Void*, callback : TemporalCoreEphemeralServerStartCallback) : Void
  fun temporal_core_ephemeral_server_free(server : TemporalCoreEphemeralServer*) : Void
  fun temporal_core_ephemeral_server_shutdown(server : TemporalCoreEphemeralServer*, user_data : Void*, callback : TemporalCoreEphemeralServerShutdownCallback) : Void

  fun temporal_core_worker_new(client : TemporalCoreClient*, options : TemporalCoreWorkerOptions*) : TemporalCoreWorkerOrFail
  fun temporal_core_worker_free(worker : TemporalCoreWorker*) : Void
  fun temporal_core_worker_validate(worker : TemporalCoreWorker*, user_data : Void*, callback : TemporalCoreWorkerCallback) : Void
  fun temporal_core_worker_replace_client(worker : TemporalCoreWorker*, new_client : TemporalCoreClient*) : TemporalCoreByteArray*
  fun temporal_core_worker_poll_workflow_activation(worker : TemporalCoreWorker*, user_data : Void*, callback : TemporalCoreWorkerPollCallback) : Void
  fun temporal_core_worker_poll_activity_task(worker : TemporalCoreWorker*, user_data : Void*, callback : TemporalCoreWorkerPollCallback) : Void
  fun temporal_core_worker_poll_nexus_task(worker : TemporalCoreWorker*, user_data : Void*, callback : TemporalCoreWorkerPollCallback) : Void
  fun temporal_core_worker_complete_workflow_activation(worker : TemporalCoreWorker*, completion : TemporalCoreByteArrayRef, user_data : Void*, callback : TemporalCoreWorkerCallback) : Void
  fun temporal_core_worker_complete_activity_task(worker : TemporalCoreWorker*, completion : TemporalCoreByteArrayRef, user_data : Void*, callback : TemporalCoreWorkerCallback) : Void
  fun temporal_core_worker_complete_nexus_task(worker : TemporalCoreWorker*, completion : TemporalCoreByteArrayRef, user_data : Void*, callback : TemporalCoreWorkerCallback) : Void
  fun temporal_core_worker_record_activity_heartbeat(worker : TemporalCoreWorker*, heartbeat : TemporalCoreByteArrayRef) : TemporalCoreByteArray*
  fun temporal_core_worker_request_workflow_eviction(worker : TemporalCoreWorker*, run_id : TemporalCoreByteArrayRef) : Void
  fun temporal_core_worker_initiate_shutdown(worker : TemporalCoreWorker*) : Void
  fun temporal_core_worker_finalize_shutdown(worker : TemporalCoreWorker*, user_data : Void*, callback : TemporalCoreWorkerCallback) : Void

  fun temporal_core_worker_replayer_new(runtime : TemporalCoreRuntime*, options : TemporalCoreWorkerOptions*) : TemporalCoreWorkerReplayerOrFail
  fun temporal_core_worker_replay_pusher_free(worker_replay_pusher : TemporalCoreWorkerReplayPusher*) : Void
  fun temporal_core_worker_replay_push(worker : TemporalCoreWorker*, worker_replay_pusher : TemporalCoreWorkerReplayPusher*, workflow_id : TemporalCoreByteArrayRef, history : TemporalCoreByteArrayRef) : TemporalCoreWorkerReplayPushResult

  fun temporal_core_complete_async_reserve(completion_ctx : TemporalCoreSlotReserveCompletionCtx*, permit_id : LibC::UInt8T) : CBool
  fun temporal_core_complete_async_cancel_reserve(completion_ctx : TemporalCoreSlotReserveCompletionCtx*) : CBool
end
