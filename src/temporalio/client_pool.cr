require "db/pool"
require "./bridge/client"
require "./bridge/runtime"

module Temporalio
  # Connection pool for Temporal clients using crystal-db's Pool implementation
  # 
  # Manages a pool of Bridge::Client connections to improve throughput by
  # allowing concurrent RPC requests across multiple connections.
  #
  # Usage:
  #   pool = ClientPool.new(
  #     target_host: "http://localhost:7234",
  #     namespace: "default",
  #     initial_pool_size: 5,
  #     max_pool_size: 10
  #   )
  #
  #   pool.checkout do |client|
  #     client.rpc_call(...)
  #   end
  class ClientPool
    @pool : DB::Pool(Bridge::Client)
    @runtime : Bridge::Runtime
    @target_host : String
    @namespace : String
    @identity : String?
    @api_key : String?
    @metadata : Hash(String, String)
    
    # Connection pool configuration
    record PoolOptions,
      initial_pool_size : Int32 = 1,
      max_pool_size : Int32 = 5,
      max_idle_pool_size : Int32 = 5,
      checkout_timeout : Time::Span = 5.seconds,
      retry_attempts : Int32 = 1,
      retry_delay : Time::Span = 1.second do
      
      def to_pool_options : DB::Pool::Options
        DB::Pool::Options.new(
          initial_pool_size: initial_pool_size,
          max_pool_size: max_pool_size,
          max_idle_pool_size: max_idle_pool_size,
          checkout_timeout: checkout_timeout.total_seconds,
          retry_attempts: retry_attempts,
          retry_delay: retry_delay.total_seconds
        )
      end
    end
    
    def initialize(
      target_host : String,
      namespace : String,
      identity : String? = nil,
      api_key : String? = nil,
      metadata : Hash(String, String) = Hash(String, String).new,
      pool_options : PoolOptions = PoolOptions.new
    )
      @target_host = target_host
      @namespace = namespace
      @identity = identity
      @api_key = api_key
      @metadata = metadata
      
      # Create shared runtime (all connections share the same runtime)
      @runtime = Bridge::Runtime.new
      
      # Create connection pool using DB::Pool
      @pool = DB::Pool(Bridge::Client).new(pool_options.to_pool_options) do
        create_connection
      end
    end
    
    # Checkout a client from the pool for use within a block
    # Automatically returns the client to the pool when done
    #
    # Usage:
    #   pool.checkout do |client|
    #     client.rpc_call(...)
    #   end
    def checkout(&block : Bridge::Client -> T) : T forall T
      @pool.checkout(&block)
    end
    
    # Get current pool statistics
    def stats : NamedTuple(total: Int32, idle: Int32, in_use: Int32)
      pool_stats = @pool.stats
      {
        total: pool_stats.open_connections,
        idle: pool_stats.idle_connections,
        in_use: pool_stats.open_connections - pool_stats.idle_connections
      }
    end
    
    # Close all connections in the pool
    def close
      @pool.close
    end
    
    private def create_connection : Bridge::Client
      options = Bridge::ClientOptions.new(
        target_url: @target_host,
        namespace: @namespace,
        identity: @identity || "#{Process.pid}@#{System.hostname}",
        api_key: @api_key,
        metadata: @metadata
      )
      
      Bridge::Client.connect(@runtime, options)
    end
  end
end
