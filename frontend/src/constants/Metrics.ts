export const Metrics = Object.freeze([
  {
    title: 'ClickHouse Table KPIs',
    metrics: [{
      expect: 'clickhouse.Query',
      metric: 'ClickHouseMetrics_Query',
    }],
  },{
    title: 'ClickHouse Node KPIs',
    metrics: [{
      expect: 'cpu usage',
      metric: '100 * (1 - sum(increase(node_cpu_seconds_total{mode="idle"}[1m])) by (instance) / sum(increase(node_cpu_seconds_total[1m])) by (instance))',
    },{
      expect: 'memory usage',
      metric: '100 * (1 - (node_memory_MemFree_bytes+node_memory_Buffers_bytes+node_memory_Cached_bytes)/node_memory_MemTotal_bytes)',
    },{
      expect: 'disk usage',
      metric: "100 * (1 - node_filesystem_avail_bytes{fstype !~'tmpfs'} / node_filesystem_size_bytes{fstype !~'tmpfs'})",
    },{
      expect: 'IOPS',
      metric: 'irate(node_disk_writes_completed_total[1m])+irate(node_disk_reads_completed_total[1m])',
    }],
  },{
    title: 'ZooKeeper KPIs',
    metrics: [{
      expect: 'znode_count',
      metric: 'znode_count',
    },{
      expect: 'leader_uptime',
      metric: 'increase(leader_uptime[1m])',
    },{
      expect: 'stale_sessions_expired',
      metric: 'stale_sessions_expired',
    },{
      expect: 'jvm_gc_collection_seconds_count',
      metric: 'jvm_gc_collection_seconds_count',
    },{
      expect: 'jvm_gc_collection_seconds_sum',
      metric: 'jvm_gc_collection_seconds_sum',
    }],
  },
]);

export const LoaderMetrics = Object.freeze([
  {
    title: 'Clickhouse Sinker KPIs',
    metrics: [
      {
        expect: 'sum by(task)(rate(clickhouse_sinker_consume_msgs_total[1m]))',
        metric: 'sum(rate(clickhouse_sinker_consume_msgs_total[1m])) by(job,task)',
      },{
        expect: 'sum by(task) (rate(clickhouse_sinker_flush_msgs_total[1m]))',
        metric: 'sum(rate(clickhouse_sinker_flush_msgs_total[1m])) by(job,task)',
      },{
        expect: 'sum by(task) (clickhouse_sinker_shard_msgs)',
        metric: 'sum(clickhouse_sinker_shard_msgs) by(job,task)',
      },{
        expect: 'sum by(task) (clickhouse_sinker_ring_msgs)',
        metric: 'sum(clickhouse_sinker_ring_msgs) by(job,task)',
      },{
        expect: 'sum by(task)(clickhouse_sinker_parsing_pool_backlog)',
        metric: 'sum(clickhouse_sinker_parsing_pool_backlog) by(job,task)',
      },{
        expect: 'sum by(task) (clickhouse_sinker_writing_pool_backlog)',
        metric: 'sum(clickhouse_sinker_writing_pool_backlog) by(job,task)',
      },
    ],
  },
]);


