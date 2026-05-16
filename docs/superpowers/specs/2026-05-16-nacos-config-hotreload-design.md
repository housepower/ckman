# Nacos 配置热生效设计

- 日期：2026-05-16
- 作者：deepclick2026@gmail.com
- 状态：草案，待评审

## 背景与目标

ckman 当前的配置仅来自本地 `conf/ckman.hjson`。Nacos 集成局限在服务发现（`naming_client.Subscribe`）和写入 `config.ClusterNodes`，配置中心能力虽已建出 `config_client`、`ListenConfig`，但 `ListenConfigCallback` 是空函数，启动期也未尝试从 Nacos 拉取配置。

本设计补齐"配置中心 + 配置热生效"两条链路：

1. **启动期**：本地解析后，尝试从 Nacos 拉取同 `dataId` 的完整配置；拉到则按 Section 合并；拉不到（错误/为空）则以本地为准，不阻塞启动。
2. **运行期**：注册 Nacos `ListenConfig`，收到 OnChange 时重新解析、合并到内存 `GlobalConfig`，并通过 applier 钩子主动重建 Log / Cron / ClickHouse 连接池。
3. **不可热更新字段**（HTTP listener、Persistent backend、Nacos 连接本身）变更**不覆盖** GlobalConfig，仅记录 WARN 提示运维手动重启。
4. **不写回**本地 hjson；Nacos 配置仅在内存生效。

参考实现：`/root/go/src/gitlab.eoitek.net/DC/cell/nacos/nacos.go` 与 `cell/ctx.go` 的同进程 reload 模式。

## 约束与决策汇总

| 决策点 | 选择 | 理由 |
|---|---|---|
| Nacos 内容格式 | 跟随本地后缀（hjson / yaml / json） | 与本地一致，运维心智低 |
| Nacos 配置结构 | 完整 `CKManConfig`，但 Nacos 段被忽略 | 避免自引用回路 |
| 启动期优先级 | Nacos 拉到就用，失败回退本地 | 直接对应用户描述 |
| Reload 形式 | 同进程选择性重建（无 `syscall.Exec`） | 无 gosec G204 告警，无 PID/FD 泄漏；与 cell 同进程 reload 思路一致 |
| 热重建组件 | Log、Cron、ClickHouse 连接池 | 重建代价低、状态隔离清晰 |
| Bootstrap 字段变更 | 不覆盖 GlobalConfig，仅 WARN | 防止内存与运行态不一致 |
| 本地反向写回 | 不写回 | 避免双向同步冲突；本地仅作 fallback |

## 整体架构

### 文件改动概览

```
config/
  config.go                # 既有 + 新增 SyncConfig / CfgNamespaceId 字段、ConfigMutex
  nacos_apply.go           # 新增：mergeFromNacos / diffBootstrap / ApplyNacosUpdate / ApplyInitialNacos / applier registry
  nacos_apply_test.go      # 新增：11 个单元测试
service/nacos/nacos.go     # 新增 PullAndMerge；ListenConfigCallback 改为调用 config.ApplyNacosUpdate
main.go                    # 启动时调用 PullAndMerge；注册三个 applier
```

### main.go 时序

```
ParseConfigFile(local)                               // 既有
Gsypt.Unmarshal                                      // 既有
log.InitLogger                                       // 既有
nacos.InitNacosClient                                // 既有
nacosClient.PullAndMerge(&GlobalConfig)              // 新增（首次拉取并 merge）
config.RegisterApplier("log", logApplier)            // 新增
repository.InitPersistent                            // 既有
backup.Init                                          // 既有
runnerServ.Start                                     // 既有
svr.Start                                            // 既有
cronSvr.Start                                        // 既有
config.RegisterApplier("cron", cronApplier(&cronSvr))// 新增（闭包持有 cronSvr 指针）
config.RegisterApplier("ck",   ckPoolApplier)        // 新增
nacosClient.Start(...)                               // 既有；OnChange 已改为 config.ApplyNacosUpdate
```

### Reload 流程

```
Nacos OnChange(namespace, group, dataId, data)
  └─ config.ApplyNacosUpdate(data, fmt)
        1. 解析 data → CKManConfig（按 fmt 用 hjson/yaml）
        2. sha256(data) 与 lastAppliedHash 比对，相同则 debug 日志后返回
        3. 取 ConfigMutex 写锁
        4. oldSnapshot := DeepCopyByGob(GlobalConfig)
        5. diffs := diffBootstrap(&GlobalConfig, &remote)  // 仅检测，不修改
        6. mergeFromNacos(&GlobalConfig, &remote)          // 仅覆盖非 bootstrap 段
        7. lastAppliedHash = hash
        8. 释放写锁
        9. 对每个 diff 输出一行 WARN
        10. 顺序串行调用每个 applier(oldSnapshot, &GlobalConfig)，applier 内自 recover
        11. log.Logger.Info("nacos config applied successfully, sha=...")
```

## 数据模型

### `CKManNacosConfig` 扩展

```go
type CKManNacosConfig struct {
    Enabled        bool
    Hosts          []string
    Port           uint64
    UserName       string `yaml:"user_name"   json:"user_name"`
    Password       string
    NamespaceId    string `yaml:"namespace_id" json:"namespace_id"`
    CfgNamespaceId string `yaml:"cfg_namespace_id" json:"cfg_namespace_id"` // 新增
    Group          string
    DataID         string `yaml:"data_id"     json:"data_id"`
    BeatInterval   int64  `yaml:"beat_interval" json:"beat_interval"`
    SyncConfig     bool   `yaml:"sync_config" json:"sync_config"`           // 新增；fillDefault=true
}
```

`fillDefault` 增加 `c.Nacos.SyncConfig = true`。

`InitNacosClient` 中 config client 创建逻辑借鉴 cell：若 `CfgNamespaceId != ""`，则 config client 用 `CfgNamespaceId` 覆盖 `NamespaceId`；特殊值 `"public"` 视为空串。

### 新增全局变量（`config` 包）

```go
var ConfigMutex sync.RWMutex   // 保护 ApplyNacosUpdate 的临界区
var lastAppliedHash string     // sha256(data) 去重
var (
    appliersMu sync.Mutex
    appliers   []namedApplier
)
type namedApplier struct { name string; fn Applier }
type Applier func(old, new *CKManConfig)
```

`lastAppliedHash` 保持包内私有；启动期 `ApplyInitialNacos` 会写入它，避免 SDK 在 listener 启动后回放首次内容触发误 reload。

## 合并语义

### 永远保留本地

| 字段 | 理由 |
|---|---|
| `ConfigFile`、`Version` | 运行时元数据，与文件位置/构建版本绑定 |
| `Nacos`（整段） | 避免自引用、避免配置中心切换中态 |

### Bootstrap 段：检测变更，不覆盖

| 字段 | 理由 |
|---|---|
| `Server.Ip` / `Server.Port` | HTTP listener 已绑定 |
| `Server.Https` / `Server.CertFile` / `Server.KeyFile` | 同上 |
| `Server.PkgPath` | `common.SetPkgPath` 启动时一次性应用 |
| `Server.PersistentPolicy` | repository backend 初始化即冻结 |
| `PersistentConfig` | 同上 |

### 其余 Section：DeepEqual 零值判定后整段覆盖

| Section | 触发条件 |
|---|---|
| `Log` | `!reflect.DeepEqual(remote.Log, CKManLogConfig{})` |
| `Cron` | `!reflect.DeepEqual(remote.Cron, CronJob{})` |
| `ClickHouse` | `!reflect.DeepEqual(remote.ClickHouse, ClickHouseOpts{})` |

`Server` 段非 bootstrap 字段（`SessionTimeout / SwaggerEnable / PublicKey / TaskInterval / Metric / MetricPath / Pprof`）：单字段非零时覆盖；这是因为 Server 段同时包含 bootstrap 与非 bootstrap，不能整段 DeepEqual 判定。

### bootstrap diff 输出

每个变更字段单独一行 WARN：

```
nacos config: bootstrap field Server.Port changed (local=8808, remote=9999), ignored in memory; restart ckman manually to apply
```

`Nacos` 整段比较，命中只输出 `"Nacos"` 一项，不展开内部字段。
`PersistentConfig` 用 `DeepEqual`，命中只输出 `"PersistentConfig"`，不展开 map key。

## Applier 实现

### 接口

```go
func RegisterApplier(name string, fn Applier)
```

执行规约：
- Idempotent：同 Section 内容不变时应等价于 noop。
- 自行 `recover`：panic 在 applier 内捕获，写错误日志，不影响后续 applier。
- 顺序串行调用：log 优先，cron 次之，ck pool 最后（保证 cron 重建时已用新 log level）。
- 在 `ConfigMutex` 写锁释放**之后**调用，避免长时间持锁。

### 三个内置 Applier

**logApplier（main.go）**

```go
func logApplier(old, new *config.CKManConfig) {
    if reflect.DeepEqual(old.Log, new.Log) { return }
    log.InitLogger(LogFilePath, &new.Log)
    log.Logger.Infof("log config reloaded: %+v", new.Log)
}
```

**cronApplier（main.go，闭包持有 `**cron.CronService`）**

```go
func cronApplier(svrPtr **cron.CronService) config.Applier {
    return func(old, new *config.CKManConfig) {
        if reflect.DeepEqual(old.Cron, new.Cron) { return }
        (*svrPtr).Stop()
        ns := cron.NewCronService(*new)
        if err := ns.Start(); err != nil {
            log.Logger.Errorf("cron reload failed: %v", err)
            return
        }
        *svrPtr = ns
        log.Logger.Infof("cron reloaded")
    }
}
```

`defer cronSvr.Stop()` 改为闭包 `defer func(){ cronSvr.Stop() }()`，确保进程退出时关闭当前实例。

**ckPoolApplier（main.go，懒重建）**

```go
func ckPoolApplier(old, new *config.CKManConfig) {
    if reflect.DeepEqual(old.ClickHouse, new.ClickHouse) { return }
    var hosts []string
    common.ConnectPool.Range(func(k, _ any) bool {
        hosts = append(hosts, k.(string)); return true
    })
    common.CloseConns(hosts)
    log.Logger.Infof("clickhouse pool reloaded: %+v", new.ClickHouse)
}
```

不主动重建 pool 对象；下次连接申请时自然使用新的 `MaxOpenConns / MaxIdleConns / ConnMaxIdleTime`。

## Nacos 客户端改动

### `PullAndMerge`（启动期）

```go
func (c *NacosClient) PullAndMerge(cfg *config.CKManConfig) error {
    if !c.Enabled || !cfg.Nacos.SyncConfig {
        return nil
    }
    content, err := c.Config.GetConfig(vo.ConfigParam{
        DataId: c.DataId,
        Group:  c.GroupName,
    })
    if err != nil {
        log.Logger.Warnf("nacos config not available, fallback to local: %v", err)
        return nil
    }
    if content == "" {
        log.Logger.Warnf("nacos config empty (dataId=%s, group=%s), fallback to local",
            c.DataId, c.GroupName)
        return nil
    }
    return config.ApplyInitialNacos([]byte(content), path.Ext(cfg.ConfigFile), cfg)
}
```

### `ListenConfigCallback`（运行期）

```go
func (c *NacosClient) listenConfigCallback(namespace, group, dataId, data string) {
    fmt := path.Ext(config.GlobalConfig.ConfigFile)
    if err := config.ApplyNacosUpdate([]byte(data), fmt); err != nil {
        log.Logger.Errorf("nacos config apply failed: %v", err)
    }
}
```

`ListenConfig` 中绑定该 callback，替换原 `ListenConfigCallback` 全局空函数。

### `Start` 改动

`Start` 中 `ListenConfig` 已存在；只需把 callback 由空函数改为上文。`SyncConfig=false` 时 `Start` 中跳过 `ListenConfig` 注册（仅做 Subscribe + RegisterInstance）。

## 错误与边界

| 场景 | 行为 |
|---|---|
| 启动时 Nacos `GetConfig` 网络错误 | WARN，沿用本地，不退出 |
| 启动时 Nacos `GetConfig` 返回空 | WARN("nacos config empty")，沿用本地 |
| OnChange 时 remote 解析失败 | ERROR，跳过本次 reload，**不覆盖** GlobalConfig |
| 单个 applier panic | 自 recover，ERROR 日志，继续下一个 |
| Nacos client 中途断连 | SDK 内部重连，后续 OnChange 推送自然恢复 |
| SDK 启动回放最后一次内容 | sha256 去重，跳过 |
| `SyncConfig=false` | 既不 `PullAndMerge`，也不注册 `ListenConfig`；服务发现保持工作 |
| `Nacos.Enabled=false` | 现有短路逻辑不变 |
| Cron 重建中 `Start` 失败 | ERROR 日志，旧 cron 已 Stop，**不回滚**；运维介入或下次 reload 修复 |

## 并发与一致性

- `ConfigMutex`：仅在 `ApplyNacosUpdate` / `ApplyInitialNacos` 的 merge 临界区持写锁。读取 `GlobalConfig` 的既有路径不加锁（沿用既有代码风格）。
- Nacos SDK 的 callback 单线程派发，多个 OnChange 串行进入 `ApplyNacosUpdate`。
- Applier 在锁外串行执行，单个 applier 内部允许阻塞（cron 重建可能耗时数百毫秒）。

## 测试

### 单元测试（`config/nacos_apply_test.go`）

| 用例 | 验证 |
|---|---|
| `TestMergeFromNacos_NonBootstrapOverrides` | Log/Cron/ClickHouse 段非零时覆盖 local |
| `TestMergeFromNacos_EmptyRemoteSectionsKeepLocal` | remote 段为零值时保留 local |
| `TestMergeFromNacos_BootstrapNeverOverridden` | Server.Port / PersistentPolicy / Nacos 段不被 remote 影响 |
| `TestMergeFromNacos_PreservesConfigFileVersion` | `ConfigFile` 和 `Version` 始终来自 local |
| `TestDiffBootstrap_DetectsAllFields` | 所有 bootstrap 字段变更都被检出 |
| `TestDiffBootstrap_NoChangeNoDiff` | 完全一致返回空切片 |
| `TestApplyNacosUpdate_SkipsOnSameHash` | 同内容连续两次只触发一次 applier |
| `TestApplyNacosUpdate_ParseError` | 非法 hjson 不修改 GlobalConfig |
| `TestApplyNacosUpdate_ApplierPanicIsolated` | 一个 applier panic 不阻断后续 applier |
| `TestApplyNacosUpdate_AppliersInRegistrationOrder` | applier 按注册顺序串行 |
| `TestParseRemote_Hjson_Yaml_Json` | 三种格式按 fmt 正确解析 |

辅助：`config.ResetForTest()` 测试用导出函数（清空 GlobalConfig / lastAppliedHash / appliers），用 `t.Cleanup` 复位。

### 人工冒烟（实施完成后）

1. `Nacos.Enabled=false` 启动，行为如旧。
2. `Enabled=true` 但 Nacos 上无 dataId，启动期 WARN，沿用本地。
3. Nacos 上有 dataId，启动期 Log/Cron/ClickHouse 从 Nacos 应用。
4. 运行期改 Nacos 的 `log.level` → 立即生效。
5. 运行期改 Nacos 的 `cron.watch_cluster_status` → 新 spec 生效。
6. 运行期改 Nacos 的 `server.port=9999` → WARN，HTTP 仍监听原端口。
7. 运行期改 Nacos 的 `nacos.password` → WARN，不重连 Nacos。
8. `SyncConfig=false` 启动 → 不 PullAndMerge / 不收 OnChange；naming 服务发现仍工作。

不写真实 Nacos 端到端集成测试（mock `config_client.IConfigClient` 接口成本过高）。

## 安全考量

- 不引入 `syscall.Exec` / `exec.Command`，无 gosec G204 触发。
- Nacos 内容**不**进入任何命令参数 / 文件路径 / 反序列化未声明字段；攻击者写入 Nacos 仅能改变 ckman 行为，不能获得任意代码执行。
- 解析错误零容忍：parse 失败立即跳过本次 reload，不进入半态 merge。
- DoS 缓解：sha256 内容去重抵御 SDK 启动回放与上游误重复推送；明确不实现"最小重启间隔"防抖（applier 自身代价已足够低）。

## 兼容性与回滚

- 现有 `ckman.hjson` 不需要任何改动即可继续工作（`SyncConfig` 默认 `true`，但 `Enabled=false` 时整条路径短路）。
- 已部署的 Nacos 服务发现行为完全不变。
- 回滚：删除 `config/nacos_apply.go`、main.go 内 `PullAndMerge` 与 `RegisterApplier` 调用、`nacos.go` 的 `PullAndMerge` 方法与 callback 替换；恢复原 `ListenConfigCallback` 空函数。无持久化副作用。

## 实施范围之外（未来工作）

- HTTP server / Persistent 的同进程热重建（需要 `repository.Close` 入口、`server` graceful rebind）。
- Nacos → 本地双向同步（当前明确不支持）。
- 配置变更审计日志（diff 序列化落地）。
- 基于角色的 Nacos 写权限校验。
