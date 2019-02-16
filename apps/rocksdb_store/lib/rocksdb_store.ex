defmodule UDB.RocksDBStore do
  alias UDB.RocksDBStore.Stream, as: DBStream
  alias UDB.RocksDBStore.DeleteRange

  @type env_handle :: :rocksdb.env_handle()
  @type sst_file_manager :: :rocksdb.sst_file_manager()
  @type db_handle :: :rocksdb.db_handle()
  @type cf_handle :: :rocksdb.cf_handle()
  @type itr_handle :: :rocksdb.itr_handle()
  @type snapshot_handle :: :rocksdb.snapshot_handle()
  @type batch_handle :: :rocksdb.batch_handle()
  @type backup_engine :: :rocksdb.backup_engine()
  @type cache_handle :: :rocksdb.cache_handle()
  @type rate_limiter_handle :: :rocksdb.rate_limiter_handle()
  @type write_buffer_manager :: :rocksdb.write_buffer_manager()
  @type cache_type :: :lru | :clock
  @type compression_type :: :snappy | :zlib | :bzip2 | :lz4 | :lz4h | :zstd | :none
  @type compaction_style :: :level | :universal | :fifo | :none
  @type compaction_pri :: :compensated_size | :oldest_largest_seq_first | :oldest_smallest_seq_first
  @type access_hint :: :normal | :sequential | :willneed | :none
  @type wal_recovery_mode :: :tolerate_corrupted_tail_records |
    :absolute_consistency |
    :point_in_time_recovery |
    :skip_any_corrupted_records
  @type column_family :: cf_handle() | :default_column_family
  @type env_type :: :default | :memenv
  @type env :: env_type() | env_handle()
  @type env_priority :: :priority_high | :priority_low
  @type block_based_table_options :: [
    {:no_block_cache, boolean()} |
    {:block_size, pos_integer()} |
    {:block_cache, cache_handle()} |
    {:block_cache_size, pos_integer()} |
    {:bloom_filter_policy, pos_integer()} |
    {:format_version, 0 | 1 | 2} |
    {:cache_index_and_filter_blocks, boolean()}
  ]
  @type merge_operator :: :erlang_merge_operator |
    :bitset_merge_operator |
    {:bitset_merge_operator, non_neg_integer()} |
    :counter_merge_operator
  @type cf_options :: [
    {:block_cache_size_mb_for_point_lookup, non_neg_integer()} |
    {:memtable_memory_budget, pos_integer()} |
    {:write_buffer_size, pos_integer()} |
    {:max_write_buffer_number, pos_integer()} |
    {:min_write_buffer_number_to_merge, pos_integer()} |
    {:compression, compression_type()} |
    {:num_levels, pos_integer()} |
    {:level0_file_num_compaction_trigger, integer()} |
    {:level0_slowdown_writes_trigger, integer()} |
    {:level0_stop_writes_trigger, integer()} |
    {:max_mem_compaction_level, pos_integer()} |
    {:target_file_size_base, pos_integer()} |
    {:target_file_size_multiplier, pos_integer()} |
    {:max_bytes_for_level_base, pos_integer()} |
    {:max_bytes_for_level_multiplier, pos_integer()} |
    {:max_compaction_bytes, pos_integer()} |
    {:soft_rate_limit, float()} |
    {:hard_rate_limit, float()} |
    {:arena_block_size, integer()} |
    {:disable_auto_compactions, boolean()} |
    {:purge_redundant_kvs_while_flush, boolean()} |
    {:compaction_style, compaction_style()} |
    {:compaction_pri, compaction_pri()} |
    {:filter_deletes, boolean()} |
    {:max_sequential_skip_in_iterations, pos_integer()} |
    {:inplace_update_support, boolean()} |
    {:inplace_update_num_locks, pos_integer()} |
    {:table_factory_block_cache_size, pos_integer()} |
    {:in_memory_mode, boolean()} |
    {:block_based_table_options, block_based_table_options()} |
    {:level_compaction_dynamic_level_bytes, boolean()} |
    {:optimize_filters_for_hits, boolean()} |
    {:prefix_transform, [{:fixed_prefix_transform, integer()} | {:capped_prefix_transform, integer()}]} |
    {:merge_operator, merge_operator()}
  ]
  @type db_options :: [
    {:env, env()} |
    {:total_threads, pos_integer()} |
    {:create_if_missing, boolean()} |
    {:create_missing_column_families, boolean()} |
    {:error_if_exists, boolean()} |
    {:paranoid_checks, boolean()} |
    {:max_open_files, integer()} |
    {:max_total_wal_size, non_neg_integer()} |
    {:use_fsync, boolean()} |
    {:db_paths, list()} |
    {:db_log_dir, :file.filename_all()} |
    {:wal_dir, :file.filename_all()} |
    {:delete_obsolete_files_period_micros, pos_integer()} |
    {:max_background_jobs, pos_integer()} |
    {:max_background_compactions, pos_integer()} |
    {:max_background_flushes, pos_integer()} |
    {:max_log_file_size, non_neg_integer()} |
    {:log_file_time_to_roll, non_neg_integer()} |
    {:keep_log_file_num, pos_integer()} |
    {:max_manifest_file_size, pos_integer()} |
    {:table_cache_numshardbits, pos_integer()} |
    {:wal_ttl_seconds, non_neg_integer()} |
    {:manual_wal_flush, boolean()} |
    {:wal_size_limit_mb, non_neg_integer()} |
    {:manifest_preallocation_size, pos_integer()} |
    {:allow_mmap_reads, boolean()} |
    {:allow_mmap_writes, boolean()} |
    {:is_fd_close_on_exec, boolean()} |
    {:skip_log_error_on_recovery, boolean()} |
    {:stats_dump_period_sec, non_neg_integer()} |
    {:advise_random_on_open, boolean()} |
    {:access_hint, access_hint()} |
    {:compaction_readahead_size, non_neg_integer()} |
    {:new_table_reader_for_compaction_inputs, boolean()} |
    {:use_adaptive_mutex, boolean()} |
    {:bytes_per_sync, non_neg_integer()} |
    {:skip_stats_update_on_db_open, boolean()} |
    {:wal_recovery_mode, wal_recovery_mode()} |
    {:allow_concurrent_memtable_write, boolean()} |
    {:enable_write_thread_adaptive_yield, boolean()} |
    {:db_write_buffer_size, non_neg_integer()}  |
    {:in_memory, boolean()} |
    {:rate_limiter, rate_limiter_handle()} |
    {:sst_file_manager, sst_file_manager()} |
    {:write_buffer_manager, write_buffer_manager()} |
    {:max_subcompactions, non_neg_integer()}
  ]
  @type options :: db_options() | cf_options()
  @type read_options :: [
    {:verify_checksums, boolean()} |
    {:fill_cache, boolean()} |
    {:iterate_upper_bound, binary()} |
    {:iterate_lower_bound, boolean()} |
    {:tailing, boolean()} |
    {:total_order_seek, boolean()} |
    {:prefix_same_as_start, boolean()} |
    {:snapshot, snapshot_handle()}
  ]
  @type write_options :: [
    {:sync, boolean()} |
    {:disable_wal, boolean()} |
    {:ignore_missing_column_families, boolean()} |
    {:no_slowdown, boolean()} |
    {:low_pri, boolean()}
  ]
  @type write_actions :: [
    {:put, binary(), binary()} |
    {:put, cf_handle(), binary(), binary()} |
    {:delete, binary()} |
    {:delete, cf_handle(), binary()} |
    {:single_delete, binary()} |
    {:single_delete, cf_handle(), binary()} |
    :clear
  ]
  @type compact_range_options :: [
    {:exclusive_manual_compaction, boolean()} |
    {:change_level, boolean()} |
    {:target_level, integer()} |
    {:allow_write_stall, boolean()} |
    {:max_subcompactions, non_neg_integer()}
  ]
  @type flush_options :: [
    {:wait, boolean()} |
    {:allow_write_stall, boolean()}
  ]
  @type iterator_action :: :first | :last | :next | :prev | binary() | {:seek, binary()} | {:seek_for_prev, binary()}
  @type backup_info :: %{
    id: non_neg_integer(),
    timestamp: non_neg_integer(),
    size: non_neg_integer(),
    number_files: non_neg_integer()
  }
  @type size_approximation_flag :: :none | :include_memtables | :include_files | :include_both
  @type range() :: DBStream.range()
  # @type range() :: {binary(), binary()}
  @type t :: %__MODULE__ {
    name: nil | binary(),
    ref: nil | db_handle(),
    status: :open | :closed | :destroyed
  }
  @type stream_type :: :forward | :backward

  defstruct name: nil, ref: nil, status: nil

  @doc """
  Create a new RocksDB database and open a connection
  """
  @spec create(name :: binary(), opts :: options()) :: {:ok, t()} | {:error, any()}
  def create(name, opts \\ []) when is_binary(name),
    do: open(name, Keyword.merge(opts, [create_if_missing: true, error_if_exists: true]))

  @spec create!(name :: binary(), opts :: options()) :: t()
  def create!(name, opts \\ []) when is_binary(name),
    do: open!(name, Keyword.merge(opts, [create_if_missing: true, error_if_exists: true]))

  @doc """
  Opens a connection to a data store
  """
  @spec open(name :: binary(), opts :: options()) :: {:ok, t()} | {:error, any()}
  def open(name, opts \\ []) when is_binary(name) do
    case :rocksdb.open(String.to_charlist(name), opts) do
      {:ok, ref} -> {:ok, %__MODULE__{name: name, ref: ref, status: :open}}
      error      -> error
    end
  end

  @spec open!(name :: binary(), opts :: options()) :: t()
  def open!(name, opts \\ []) when is_binary(name) do
    {:ok, conn} = open(name, opts)
    conn
  end

  @doc """
  Close the connection to a data store
  """
  @spec close(conn :: t()) :: {:ok, t()} | {:error, any()}
  def close(%__MODULE__{ref: dbref, status: :open} = conn) do
    case :rocksdb.close(dbref) do
      :ok   -> {:ok, %{conn | ref: nil, status: :closed}}
      error -> error
    end
  end

  @spec close!(conn :: t()) :: t()
  def close!(%__MODULE__{status: :open} = conn) do
    {:ok, conn} = close(conn)
    conn
  end

  @spec get(conn :: t(), key :: binary(), default :: binary(), opts :: read_options()) :: nil | binary() | {:error, any()}
  def get(%__MODULE__{status: :open} = conn, key),
    do: get(conn, key, nil, [])
  def get(%__MODULE__{status: :open} = conn, key, default) when is_binary(default),
    do: get(conn, key, default, [])
  def get(%__MODULE__{status: :open} = conn, key, opts) when is_list(opts),
    do: get(conn, key, nil, opts)
  def get(%__MODULE__{ref: dbref, status: :open}, key, default \\ nil, opts \\ []) do
    case :rocksdb.get(dbref, key, opts) do
      {:ok, value} -> value
      :not_found   -> default
      error        -> error
    end
  end

  @spec get(conn :: t(), query :: DBStream.query(), opts :: read_options()) :: any() | {:error, any()}
  def stream(%__MODULE__{status: :open} = conn, query, opts \\ []),
    do: DBStream.create(conn, query, opts)

  @spec write(conn :: t(), updates :: write_actions(), opts :: write_options()) :: :ok | {:error, any()}
  def write(conn, updates, opts \\ [])
  def write(%__MODULE__{ref: dbref, status: :open}, update, opts) when is_tuple(update),
    do: :rocksdb.write(dbref, [update], opts)
  def write(%__MODULE__{ref: dbref, status: :open}, updates, opts) when is_list(updates),
    do: :rocksdb.write(dbref, updates, opts)

  @spec put(conn :: t(), key :: binary(), value :: binary(), opts :: write_options()) :: :ok | {:error, any()}
  def put(%__MODULE__{ref: dbref, status: :open}, key, value, opts \\ []),
    do: :rocksdb.put(dbref, key, value, opts)

  @spec delete(conn :: t(), key :: binary(), opts :: write_options()) :: :ok | {:error, any()}
  def delete(%__MODULE__{ref: dbref, status: :open}, key, opts \\ []) when is_binary(key),
    do: :rocksdb.delete(dbref, key, opts)

  @spec delete_range(conn :: t(), query :: Query.t(), opts :: read_options()) :: any() | {:error, any()}
  def delete_range(%__MODULE__{status: :open} = conn, query, opts \\ []) do
    DeleteRange.execute(conn, query, opts)
  end

  @spec is_empty?(conn :: t()) :: boolean()
  def is_empty?(%__MODULE__{ref: dbref, status: :open}),
    do: :rocksdb.is_empty(dbref)

  @doc """
  Destroy and delete a data store. This can not be undone.
  Will close the connection if it is open.
  """
  @spec destroy(name :: binary(), opts :: db_options()) :: :ok | {:error, any()}
  @spec destroy(conn :: t(), opts :: db_options()) :: t()
  def destroy(conn, opts \\ [])
  def destroy(name, opts) when is_binary(name), do:
    :rocksdb.destroy(String.to_charlist(name), opts)
  def destroy(%__MODULE__{status: :open} = conn, opts),
    do: conn |> close! |> destroy(opts)
  def destroy(%__MODULE__{name: name, status: :closed} = conn, opts) do
    :ok = destroy(name, opts)
    %{conn | status: :destroyed}
  end

  @doc """
  Repair a data store. Will close the connection if the input is a connection.
  """
  @spec destroy(conn :: t(), opts :: db_options()) :: t()
  def repair(conn, opts \\ [])
  def repair(%__MODULE__{status: :open} = conn, opts),
    do: conn |> close! |> repair(opts)
  def repair(%__MODULE__{name: name, status: :closed} = conn, opts) do
    :ok = :rocksdb.repair(String.to_charlist(name), opts)
    conn
  end
end

defimpl Collectable, for: UDB.RocksDBStore do
	def into(%UDB.RocksDBStore{status: :open} = conn) do
		{conn, fn
			_, {:cont, {key, value}} -> UDB.RocksDBStore.put(conn, key, value)
			_, {:cont, key} when is_binary(key) -> UDB.RocksDBStore.put(conn, key, key)
			_, :done -> conn
			_, :halt -> :ok
		end}
	end
end
