# cron

定时任务配置。使用标准的 [cron 表达式](https://en.wikipedia.org/wiki/Cron) 格式：

```
Second | Minute | Hour | DayOfMonth | Month | DayOfWeek
```

## 配置项

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `enabled` | `true` | 是否开启定时任务总开关 |
| `sync_logic_schema` | `0 * * * * ?` | 同步逻辑集群表 schema，1 分钟一次 |
| `watch_cluster_status` | `0 */3 * * * ?` | 监控 tgz 集群节点状态，异常自动拉起，3 分钟一次 |
| `sync_dist_schema` | `30 */10 * * * ?` | 同步集群内物理表 schema，10 分钟一次 |
| `clear_znodes` | — | 清理 ZooKeeper 中过期 znode 的任务 |

## 多实例时的行为

::: warning Master Only
当 `nacos.enabled = true` 时，定时任务**只在 master 实例执行**，避免重复触发。Master 选举由 Nacos 协调，详见[高可用部署](/deploy/high-availability)。
:::

## 任务说明

### sync_logic_schema

`logic_cluster` 模式下，每个物理集群可能有不一致的本地表。这个任务定时同步缺失的 schema，确保跨集群分布式表能正常工作。

### watch_cluster_status

对于 tar.gz 部署的集群，如果某个节点的 ClickHouse 进程意外退出，CKMAN 会通过 SSH 重新拉起。
::: tip
仅对 `mode = deploy` 的集群生效。`import` 模式集群不会被自动拉起，避免误操作。
:::

### sync_dist_schema

定时检查集群内分布式表与本地表的 schema 是否一致，发现差异会自动同步。

### clear_znodes

定期清理 ZooKeeper 中的孤儿 znode，避免长期运行后 ZK 数据膨胀。

## 示例

```hjson
"cron": {
  "enabled": true,
  "sync_logic_schema":    "0 * * * * ?",
  "watch_cluster_status": "0 */3 * * * ?",
  "sync_dist_schema":     "30 */10 * * * ?"
}
```

## 临时关闭

排查问题或维护期可临时关掉：

```hjson
"cron": {
  "enabled": false
}
```

修改后需要**重启** CKMAN 生效。
