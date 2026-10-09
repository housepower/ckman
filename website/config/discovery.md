# registry / config_center

注册中心与配置中心是两个独立的选择，后端可以任意组合，例如 Nacos + Nacos、Consul + Apollo、Consul + 不启用。

| 段 | 字段 | 可选值 | 说明 |
| --- | --- | --- | --- |
| `registry` | `type` | `nacos` / `consul` / `none` | 注册中心。多实例部署时启用，只有主节点执行定时任务 |
| `registry` | `service_name` | 默认取 `nacos.data_id`，即 `ckman` | 注册的服务名，同一集群的实例必须一致 |
| `config_center` | `type` | `nacos` / `consul` / `apollo` / `none` | 配置中心。启动时拉取配置，运行中热更新 |

后端的连接参数写在以后端命名的同级段里：[nacos](/config/nacos)、`consul`、`apollo`（见下文）。同一个后端同时承担两种角色时，两边共用同一个段。

## 示例

Consul 做注册中心，Apollo 做配置中心：

```hjson
"registry":      { "type": "consul" },
"config_center": { "type": "apollo" },
"consul": { "address": "127.0.0.1:8500" },
"apollo": { "meta": "http://10.0.0.30:8080", "namespace": "ckman.json" }
```

## 旧配置兼容

没有写 `registry` / `config_center` 段时，按旧的 `nacos` 段推导，旧配置文件不用修改：

| 旧配置 | 等价于 |
| --- | --- |
| `nacos.enabled: true` | `registry.type: nacos`，且 `sync_config` 不为 `false` 时 `config_center.type: nacos` |
| `nacos.enabled: false` 或未写 | 两者都是 `none` |

显式写了新段后，`nacos.enabled` 不再起作用。

## consul

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `address` | `127.0.0.1:8500` | Consul 地址。**推荐指向本机 consul agent**：服务注册挂在 agent 节点上，直连 server 时那台 server 宕机会连带丢失注册信息 |
| `scheme` | `http` | `http` / `https` |
| `datacenter` | — | 数据中心，默认取 agent 所在的 |
| `token` | — | ACL token，支持 `ENC()` 加密 |
| `check_interval` | `10s` | 心跳间隔，必须小于 `check_ttl` |
| `check_ttl` | `30s` | 超过该时长未收到心跳，实例被判为不健康，从成员列表中摘除 |
| `deregister_after` | `1m` | 不健康超过该时长后从 Consul 中删除，防止进程异常退出后残留僵尸实例 |
| `wait_time` | `30s` | 订阅变更时单次长轮询的最长等待时间 |
| `kv_key` | `ckman/ckman.hjson` | `config_center.type` 为 `consul` 时存放配置的 KV key，后缀（`.hjson` / `.json` / `.yaml`）决定内容格式 |

环境变量 `CONSUL_ADDR` 可覆盖 `address`。

Consul KV 不保留修改历史，需要回滚时请借助外部工具（如把配置放在 git 中，用 git2consul 同步）。

## apollo

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `meta` | — | Config Service 地址（默认部署下与 Meta Server 相同），多个用逗号分隔，依次重试 |
| `app_id` | `ckman` | AppId |
| `cluster` | `default` | 集群名，可按现场/环境区分 |
| `namespace` | `ckman.json` | **必须是 json 或 yaml 类型**的 namespace，整份配置写在其中。json 是合法的 hjson，可直接沿用本地配置写法（去掉注释） |
| `secret` | — | 访问密钥，支持 `ENC()` 加密 |

环境变量 `APOLLO_META` 可覆盖 `meta`。

- 请求会带上 `server.ip`，可在 Apollo 上按 IP 灰度发布，先在一台实例上验证配置。
- 每次成功拉取后，配置会备份到日志目录下的 `apollo/` 中。Apollo 不可达时用备份启动；连备份也没有时回退到本地配置，启动不会阻塞。
- ckman 只读取 Apollo，不往 Apollo 写配置。

## 注意事项

- 远端配置只会覆盖 `log`、`cron`、`clickhouse` 以及 `server` 中的部分字段。`registry`、`config_center`、`nacos`、`consul`、`apollo` 等段在远端修改无效，日志会提示需要重启。
- **切换注册中心类型必须整个集群一起切**。部分实例在 Nacos、部分在 Consul 时，两边会各自选出主节点，定时任务会重复执行。
- 切换配置中心类型时，先把配置内容同步到新的配置中心，再逐台重启。
- 注册中心刚重启、实例尚未重新注册的短暂窗口内（约一个心跳间隔），各实例可能都认为自己是主节点。
