# clickhouse

CKMAN 连接 ClickHouse 时使用的**连接池**配置。注意这是 CKMAN 自己的连接池，不是被管控 ClickHouse 集群的配置。

## 配置项

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `max_open_conns` | int | 每个 ClickHouse 节点最大可打开的连接数 |
| `max_idle_conns` | int | 每个 ClickHouse 节点最大空闲连接数 |
| `conn_max_idle_time` | int | 单个连接的最大空闲时间（秒） |

## 示例

```hjson
"clickhouse": {
  "max_open_conns": 10,
  "max_idle_conns": 2,
  "conn_max_idle_time": 10
}
```

## 调优建议

- **小集群（< 5 节点）**：默认值即可
- **大集群（> 20 节点）+ 高并发**：可以适当调高 `max_open_conns` 到 20-30，避免连接饥饿
- 注意 ClickHouse 节点本身有 `max_connections` 配置（默认 4096），CKMAN 的连接池只是 CKMAN 端的限流
- 如果 ckman.log 中出现 `too many open files` 错误，先检查 ulimit，再考虑降低 `max_open_conns`
