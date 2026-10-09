# 注册中心与配置中心拆分设计

- 日期：2026-10-09
- 状态：第一步（纯重构）已实现；第二步（Consul / Apollo）待实施

## 背景与目标

ckman 目前用 Nacos 同时做服务注册发现和配置中心，两者在配置模型、生命周期和开关语义上绑在一起。现在要支持 Consul（注册发现）和 Apollo（配置中心），如果不拆分，只能在 `service/nacos` 里堆 `if type == ...` 分支；如果直接拆，又会破坏已部署环境的 `ckman.hjson`。

本设计的目标：

1. 把"注册发现"和"配置中心"抽象成两个互相独立的接口，后端可以任意组合，例如 Nacos+Nacos、Consul+Apollo、Consul+none。
2. **旧配置文件零修改**：现有 `nacos` 段语义不变，升级后行为与升级前完全一致。
3. 新增后端只增加文件，不修改已有实现。

非目标：本期不实现 Consul KV 的配置历史与回滚，不做注册中心的在线热切换（切换注册中心或配置中心类型都需要重启）。

## 现状：耦合点盘点

naming client 和 config client 本来就是两个独立的 SDK 对象，代码层面的耦合只有以下四处：

| # | 耦合点 | 位置 |
|---|---|---|
| 1 | **配置段共用**：连接参数（hosts/port/user_name/password）与两种能力各自的参数（`namespace_id` / `cfg_namespace_id` / `sync_config` / `beat_interval`）混在同一个 `CKManNacosConfig` 里 | `config/config.go:86` |
| 2 | **生命周期共用**：`NacosClient.Start()` 一次完成 Subscribe、ListenConfig、Register 三件事 | `service/nacos/nacos.go` `Start` |
| 3 | **开关语义被借用**：`IsMasterNode()` 用 `Nacos.Enabled` 判断是否处于多实例模式 | `config/config.go` `IsMasterNode` |
| 4 | **标识复用**：服务名 `ServiceName` 直接取的是 `DataID` | `service/nacos/nacos.go` `InitNacosClient` |

外围引用：`main.go`（`InitNacosClient` / `PullAndMerge` / `Start`）、`cmd/ckman-watchdog`（`checkNacosDep` 对 Nacos 端口做 TCP 探测）、`config.MergeEnv`（`NACOS_HOST` 环境变量）、`controller/config.go`（`GetInstances` 读 `ClusterNodes`）。

结论：真正要拆的是**配置模型**和**启动编排**，SDK 调用本身基本不用改。

## 核心思路：连接层与能力层分离

Nacos 仍然是"一个连接、提供两种能力"，这种绑定放在**连接层**是合理的。过去的问题在于**能力层**也跟着绑在了一起。拆分后：

- **能力层**：两个接口 `Registry` 和 `ConfigSource`，上层代码只依赖它们。
- **连接层**：每个后端一个配置段（`nacos` / `consul` / `apollo`），同一个后端的两种能力实现读取同一个段，连接参数不用写两遍。
- **选择层**：`registry` / `config_center` 两个新段只决定"用哪个后端"，并携带少量与后端无关的通用参数。

## 接口定义

```go
package discovery

// Registry 负责注册本实例并维护集群成员列表。
type Registry interface {
    // Start 订阅成员变更并注册本实例；变更通过 OnChange 回调通知。
    Start(self config.ClusterNode) error
    // Stop 反注册并释放资源；必须 best-effort，任一步失败也要继续执行后续步骤。
    Stop() error
    // OnChange 设置成员变更回调，必须在 Start 之前调用。回调收到的是完整的成员快照。
    OnChange(func([]config.ClusterNode))
}

// ConfigSource 负责拉取并监听远端配置。format 取值为 config.FORMAT_*。
type ConfigSource interface {
    // Load 拉取一次完整配置。远端没有配置时返回空内容、nil 错误。
    Load() (data []byte, format string, err error)
    // Watch 开始监听变更，回调收到的是完整内容。
    Watch(func(data []byte, format string)) error
    Close() error
}
```

成员直接复用 `config.ClusterNode`，不另起类型。接口里不放 `Endpoints()`：watchdog 不启动 SDK，后端地址由纯函数 `config.DiscoveryEndpoints` 提供。

要点：

- **`ConfigSource` 的返回值带上格式。** 现在解析格式是看本地 `ConfigFile` 的后缀（`ListenConfigCallback`、`PullAndMerge`），换成 Apollo 的 json/yaml 命名空间就会出错。改成由数据源自己告知格式：Nacos 实现继续返回本地后缀，行为与现在一致；Apollo 按命名空间类型返回；Consul KV 按 key 的后缀返回。
- **类型为 `none` 时返回空实现**，不返回 nil。上层不需要判空：空的 `Registry` 在 `Start` 时直接回调一次 `[self]`，空的 `ConfigSource` 在 `Load` 时返回空内容。

## 包结构

```
service/discovery/
  discovery.go       # Registry / ConfigSource 接口
  factory.go         # NewRegistry / NewConfigSource：按 type 分派，none → noop 实现；
                     # LoadInitial（启动拉取，原 PullAndMerge）/ Watch（热更新）
  noop.go
  nacos/
    conn.go          # 由 CKManNacosConfig 构造 ClientConfig/ServerConfigs（从 InitNacosClient 抽出）
    registry.go      # naming client：Register/Subscribe/Deregister
    source.go        # config client：GetConfig/ListenConfig
  consul/
    registry.go      # TTL 健康检查 + blocking query，要点见下文
    source.go        # KV + blocking query（可选，第二步）
  apollo/
    source.go        # agollo v4
```

`service/nacos/` 在第一步重构完成后删除，逻辑迁到 `service/discovery/nacos/`。

`config/nacos_apply.go` 的合并、applier、hash 去重**原样保留**，只做改名：`ApplyNacosUpdate` → `ApplyRemoteUpdate`，`ApplyInitialNacos` → `ApplyInitialRemote`，`mergeFromNacos` → `mergeFromRemote`，`SetNacosLogger` → `SetRemoteLogger`。文件改名为 `remote_apply.go`。这些函数只在本仓库内调用，不保留旧名别名。

## 配置模型

### 新格式

```hjson
registry: {
  type: "nacos"            // nacos | consul | none
  service_name: "ckman"    // 可选；默认见兼容规则
}
config_center: {
  type: "apollo"           // nacos | apollo | consul | none
}
nacos: { ...与现在完全一致... }
consul: {
  address: "127.0.0.1:8500"
  scheme: "http"
  datacenter: ""
  token: "ENC(...)"
  check_interval: "10s"
  check_ttl: "30s"
  deregister_after: "1m"
  kv_key: "ckman/ckman.hjson"   // 仅 config_center.type=consul 时使用
}
apollo: {
  meta: "http://apollo-config:8080"
  app_id: "ckman"
  cluster: "default"
  namespace: "ckman.json"       // 建议 json 类型：JSON 是合法的 hjson，现有解析器可直接使用
  secret: "ENC(...)"
  backup_dir: ""                // 默认 logs 目录，与 nacos cache_dir 一致
}
```

`CKManConfig` 新增 `Registry`、`ConfigCenter`、`Consul`、`Apollo` 四个字段，`Nacos` 字段保留不动。

### 兼容规则：`normalizeDiscovery()`

在 `config` 包新增 `normalizeDiscovery(c *CKManConfig)`，由 `ParseConfigFile` 在 `fillDefault` → 解析文件 → `MergeEnv` 之后调用，**只调用这一次**。所有命令（ckman、watchdog、ckmanctl 等）都经过 `ParseConfigFile`，因此都能拿到推导后的 type。解析发生在 logger 初始化之前，告警先存进 `config.ConfigWarnings`，由 `main.go` 在 logger 就绪后输出。之后业务代码不再读取 `Nacos.Enabled` 和 `Nacos.SyncConfig`。

| 条件 | 推导结果 |
|---|---|
| `registry.type` 为空，且 `nacos.enabled=true` | `registry.type = nacos` |
| `registry.type` 为空，且 `nacos.enabled=false` | `registry.type = none` |
| `config_center.type` 为空，`nacos.enabled=true` 且 `sync_config=true` | `config_center.type = nacos` |
| `config_center.type` 为空，其他情况 | `config_center.type = none` |
| `registry.service_name` 为空 | 取 `nacos.data_id`（默认 `ckman`），与现在一致 |
| 显式写了新段，且 `nacos.enabled=true` 但两个 type 都不是 nacos | 以新段为准，打 WARN（新格式里 `nacos.enabled` 可以不写，所以不对 false 告警） |
| `type` 取值非法 | 启动失败（Fatal），不静默回退 |

注意 `fillDefault` 里 `SyncConfig` 的默认值是 `true`，因此旧配置只要打开了 `nacos.enabled`，就会同时得到注册中心和配置中心，与现在的行为一致。

`nacos.enabled` 字段保留下来，作为旧配置的输入；新部署的配置文件模板（`resources/ckman.hjson`）改为写 `registry` / `config_center`，`nacos.enabled` 在模板里删除。

### 环境变量

- `NACOS_HOST` 照旧生效。
- 新增 `CONSUL_ADDR`、`APOLLO_META`，规则与 `NACOS_HOST` 一致：只覆盖地址，不改变 type。

## 启动编排

现在 `main.go` 的时序是：

```
InitNacosClient → PullAndMerge → 注册 log applier → ... → HTTP server → cron → 注册 cron/ck applier → nacosClient.Start（Subscribe + ListenConfig + Register）
```

拆分后：

```
source   := discovery.NewConfigSource(cfg)          // 原 InitNacosClient 的 config 部分
data, f  := source.Load() → config.ApplyInitialRemote(data, f, cfg)
注册 log applier
... backup / runner / HTTP server / cron ...
注册 cron / ck applier
source.Watch(config.ApplyRemoteUpdate)              // 必须在所有 applier 注册之后
registry := discovery.NewRegistry(cfg)
registry.OnChange(setClusterNodes)
registry.Start(self)
handleSignal → registry.Stop(); source.Close()
```

两个约束：

1. **`source.Watch` 必须在所有 applier 注册完之后才能调用**，与现在 `ListenConfig` 放在 `Start()` 里的时序一致。否则早到的推送只会触发 log applier，cron 和 ck 连接池会漏掉这次变更。
2. **`registry.Start` 必须在 HTTP server 起来之后调用**：注册出去的实例必须能被访问到。

顺带修复：现在 `main.go` 退出时不调用 `nacosClient.Stop()`，实例要等心跳超时才会被摘除。拆分后在 `handleSignal` 返回后调用 `registry.Stop()`，对 Consul 来说更重要，因为它没有临时实例的概念。

## 成员列表与主节点

`setClusterNodes` 统一负责写入 `config.ClusterNodes`：

- **按 `ip:port` 排序后再写入（实现为 `config.SetClusterNodes`）。** 现在 `IsMasterNode()` 取 `ClusterNodes[0]`，而 Nacos 回调和 Consul blocking query 返回的顺序都不保证稳定，可能出现两个节点同时认为自己是主节点。排序后至少保证各节点在同一份列表下得出同一个结论。
- **始终把 self 并进去**，去重。`controller/config.go` 的 `GetInstances` 里现有的 union 逻辑下沉到这里。

`IsMasterNode()` 改为：

```go
if GlobalConfig.Registry.Type == "none" {
    return true
}
```

语义与现在一致：没有注册中心就视为单机，自己就是主节点。

后续可选：用 Consul session + `KV Lock` 做真正的选主，作为 `Registry` 的扩展接口（`Leader() bool`），不在本期范围内。

### 已知问题（独立修复）

`GetClusterPeers()`（`config/config.go:210`）的条件是 `Ip != node.Ip && Port != node.Port`。多实例部署的端口通常相同，这个条件对所有节点都不成立，结果 peers 永远为空，`controller/package.go` 上传的安装包不会同步到其他节点。应改为 `!(Ip == node.Ip && Port == node.Port)`。这个问题与本设计无关，单独提交修复。

## 远端配置的安全边界

`mergeFromNacos` 是**按段白名单**覆盖的：只覆盖 Log、Cron、ClickHouse，以及 Server 段内的部分字段，其余一律不覆盖。新增的 `Registry` / `ConfigCenter` / `Consul` / `Apollo` 四段**天然不会被远端覆盖**，不存在"配置中心下发一份配置，把配置中心自己换掉"的循环。（已核对 `config/nacos_apply.go` 的 `mergeFromNacos`。）

需要补的地方：

- `diffBootstrap` 目前只对 `Nacos` 段做差异告警，要加上这四个新段，让远端改了这些段时能打出"需要重启"的 WARN。
- 补测试：远端配置里带 `registry.type` / `config_center.type`，断言合并后本地值不变，并且有 WARN。

## 各后端实现要点

### Nacos（第一步，纯重构）

- `conn.go` 从 `InitNacosClient` 中抽出 ClientConfig/ServerConfigs 的构造逻辑，`registry.go` 和 `source.go` 各自按需创建 naming client 或 config client。注册中心选 Consul、配置中心选 Nacos 时，就只创建 config client。
- 两个实现分别使用 `namespace_id` 和 `cfg_namespace_id`，逻辑与现在一致。
- 顺带修正：`DeregisterInstance` 传的是 `Ephemeral: false`，而注册时是 `true`，两者应保持一致。

### Consul 注册发现（第二步）

实现要点：

1. `AgentServiceCheck` 设置 `DeregisterCriticalServiceAfter`（配置项 `deregister_after`，默认 `1m`），防止进程被 kill -9 后僵尸实例一直留在 catalog 里。
2. 不把实例信息整体序列化写进 Service Meta：Consul 限制单个 meta 值最长 512 字符。
3. TTL 续约 goroutine 与实例生命周期绑定：反注册时停止续约，重复注册时先取消旧的续约。
4. blocking query 的 `wait_time` 只在配置默认值一处定义（`30s`），代码里不另设兜底值。
5. 配置使用 ckman 的强类型结构，时长用 `time.ParseDuration` 解析，日志用 `log.Logger`。
6. 部署约定：Consul 的服务注册挂在 agent 节点上。推荐每台 ckman 机器跑一个本地 consul agent，`address` 指向 `127.0.0.1:8500`。如果直接连 server，那台 server 宕机时注册信息会跟着丢失。这一点写进部署文档。

### Apollo 配置中心（第二步）

- 使用 `github.com/apolloconfig/agollo/v4`。
- `Load`：`GetConfig(namespace).GetValue("content")`。json/yaml 类型的命名空间会把整份内容存在 `content` 这个 key 下。
- `Watch`：`AddChangeListener`，只处理 `content` 变更，把新值交给回调。
- `Format`：由命名空间后缀决定，`.json` → `FORMAT_JSON`，`.yaml`/`.yml` → `FORMAT_YAML`。不支持 properties 类型，配置时报错。
- Apollo 客户端是只读的，ckman 不往 Apollo 写配置。`NacosClient.PublishConfig` 目前没有调用方，不放进 `ConfigSource` 接口。

### Consul KV 配置中心（可选）

用 KV 加 blocking query 实现，写法与 Consul 注册发现里的 Subscribe 循环相同。Consul KV 不保留历史，文档里说明"回滚需要依赖外部工具（如 git2consul）"。

## watchdog

`cmd/ckman-watchdog` 的 `checkNacosDep` 泛化为 `depCheck` 里的注册中心/配置中心探测：`config.DiscoveryEndpoints(cfg)` 是一个纯函数，返回 `map[后端类型][]地址`，watchdog 对每个后端的地址做 TCP 拨号，任一可达即视为可用。watchdog 不启动 SDK，避免注册副作用。

watchdog 通过 `ParseConfigFile` 加载配置，自动经过 `normalizeDiscovery`。

## 运维约束（写进文档）

1. **切换注册中心类型必须整个集群一起切。** 如果一部分实例在 Nacos、一部分在 Consul，两边各自选出主节点，定时任务会被执行两次。
2. 切换配置中心类型时，先把配置内容同步到新的配置中心，再逐台重启。
3. 从旧版本滚动升级到新版本时，Nacos 侧的服务名、group、namespace 都不变，新旧版本的实例能互相看见，可以逐台升级。

## 实施步骤

### 第一步：纯重构，行为不变（已完成）

1. 新增 `service/discovery` 接口、factory、noop 实现。
2. 把 Nacos 拆成 `registry.go` 和 `source.go`，删除 `service/nacos`。
3. `config` 包：新增 `Registry` / `ConfigCenter` 段和 `normalizeDiscovery()`，`IsMasterNode` 改用 `Registry.Type`；`nacos_apply.go` 改名；`diffBootstrap` 补上新段。
4. `main.go` 按新的启动编排改写，退出时调用 `registry.Stop()`。
5. watchdog 泛化依赖探测。

验收标准：

- 现有测试全部通过。
- `normalizeDiscovery()` 单元测试覆盖兼容规则表格里的每一行。
- 用一份**未修改**的旧 `ckman.hjson`，连真实 Nacos 起两个实例：成员列表、主节点判断、配置热更新（Log/Cron/ClickHouse）、进程退出后实例被摘除，这几项与重构前行为一致。
  - 实测（nacos-server v2.4.3，2026-10-09）：两个实例互相可见；启动拉取与热更新生效；远端下发 `registry` 段被拦截并打 WARN；SIGTERM 后约 0.5 秒对端成员列表即更新（重构前不反注册，要等心跳超时）。
  - 本机设了 `http_proxy` 时，curl 调 Nacos OpenAPI 要加 `--noproxy '*'`，ckman 进程也要去掉代理环境变量再启动。

### 第二步：新增后端

1. `consul/registry.go`（含上文 6 项修补）。
2. `apollo/source.go`。
3. 可选：`consul/source.go`。
4. 更新 `resources/ckman.hjson` 模板和 website 文档。

验收标准：

- Consul：两个实例互相可见；kill -9 一台后，在 `check_ttl` 时间内从成员列表消失，`deregister_after` 之后从 catalog 消失；主节点切换正确。
- Apollo：启动拉取成功；在 Portal 发布后热更新生效；Apollo 不可达时，用本地备份或本地配置启动，不阻塞。
- 组合测试：Consul + Apollo、Consul + none、Nacos + Apollo 三种组合都能正常启动。

## 工作量估算

| 部分 | 估算 |
|---|---|
| 第一步：重构 + 兼容 + 测试 | 1.5～2 人天 |
| Consul 注册发现 | 1～1.5 人天 |
| Apollo 配置中心 | 约 1 人天 |
| Consul KV 配置中心（可选） | 约 0.5 人天 |
