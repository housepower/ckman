# ckman watchdog 自愈守护程序 设计

日期：2026-06-15
形态：复用 ckman 自有 Go 包的独立二进制 `cmd/watchdog`（源文件 `cmd/watchdog/watchdog.go`，沿用 `cmd/ckmanctl/ckmanctl.go` 的单文件命名约定），由 crontab 每分钟调起，一次性运行。
参照实现：`gitlab.eoitek.net/DC/cell/cmd/watchdog/main.go`（结构高度一致，本设计为其在 ckman 上的移植）。

## 目标

为 tarball 部署形态的 ckman 提供进程级自愈：

1. 进程不存在 → 自动拉起；
2. 进程在但夯住（持续 D 态 / T 态 / HTTP 接口无应答）→ `kill -9` 后重启；
3. 进程在但接口非健康 / 依赖（数据库、Nacos）不可用 → **仅告警，不干预**；
4. 进程不存在时区分「运维主动停」与「异常崩溃」：主动停不干预；
5. 依赖不可用时不重启（重启无用且会造成重启风暴），只写日志文件。

**零配置**：不新增任何配置段，不改 `config.go` / `ckman.hjson` 模板。watchdog 自推导路径、读取**现有** `ckman.hjson` 获取端口/协议/Nacos/持久化策略，其余调参用常量默认值（可选 flag 覆盖）。

## 非目标

- 不针对 systemd 形态（systemd 自带 `Restart=always`，本程序面向 tarball 解压包）。
- 不做主动的优雅重启 / 配置热加载。
- 不引入新的第三方依赖（procfs / zap / lumberjack / go-daemon 均已在 go.mod）。

## 关键事实（已核对源码）

- 启动：`<HDIR>/bin/start` 内部执行 `ckman -c=... -p=<HDIR>/run/ckman.pid -l=... -d`（`-d` 已内置）。
- pidfile：由 go-daemon（`gopkg.in/sevlyar/go-daemon.v0`）写入。优雅退出（SIGTERM/SIGINT，即 `bin/stop` / `kill`）触发 `cntxt.Release()` → **删除 pidfile**；崩溃 / OOM / `kill -9` → pidfile **残留**。这是区分「主动停」与「崩溃」的判据。
- HTTP 探活端点：`cfg.Server.MetricPath`（默认 `/metrics`，`cfg.Server.Metric` 默认 true）。该端点**不经过 JWT 中间件，无需鉴权** —— 探活天然绕过鉴权。`/metrics` 通即证明 HTTP server goroutine 存活（纯 prometheus handler，不碰 DB/Nacos）。
- 配置读取：`config.ParseConfigFile(path, version)` 即可。**不需要 `common.Gsypt.Unmarshal` 解密** —— watchdog 只用明文且稳定的字段（port/https/metric/metric_path/persistent_policy、网络库 host:port、nacos hosts:port），全程无鉴权，不读任何密码。少一个失败路径，也降低版本耦合。
- 持久化健康：**不调用 `repository.InitPersistent()`**。原因：sqlite adapter 的 `Init()` 会 `AutoMigrate`（DDL 写）+ legacy 迁移（`repository/sqlite/sqlite.go:75`、`migrate.go`），watchdog 每分钟跑一次会对 ckman 正在持有的同一 sqlite 文件反复写迁移（即便 WAL 也会 `SQLITE_BUSY`/争 schema）。改为零副作用探测（见「依赖深探」）。
- 日志：`github.com/housepower/ckman/log` 包，`log.InitLogger(path, &cfg.Log)` + 全局 `log.Logger`。
- `cmd/` 目录已有 `cmd/ckmanctl`、`cmd/upgrade` 先例。

## 架构

单文件 `cmd/watchdog/watchdog.go`，`package main`，一次性运行后退出（无常驻）。

### 路径推导（derivePaths）

照 `bin/start` 风格：`exe=os.Executable()`，`DIR=dir(exe)`，`HDIR=dir(DIR)`。

| 路径 | 默认值 | flag 覆盖 |
|------|--------|----------|
| conf | `<HDIR>/conf/ckman.hjson` | `-c` |
| pidfile | `<HDIR>/run/ckman.pid` | `-pid` |
| ckmanBin | `<HDIR>/bin/ckman` | — |
| startCmd | `<HDIR>/bin/start` | — |
| watchdogLog | `<HDIR>/logs/watchdog.log` | — |
| alertLog | `<HDIR>/logs/ckman-alert.log` | — |
| restartsFile | `<HDIR>/run/watchdog.restarts` | — |
| lockFile | `<HDIR>/run/watchdog.lock` | — |

### 真相源 = 进程身份（procfs），pidfile 仅作缺失原因判据

`findCkmanPids(p)` 用 `procfs.AllProcs()` 按身份扫描（**不依赖 pidfile 判存活**）：

1. 主匹配：`/proc/<pid>/exe`（内核规范路径，软链已消解）经 `filepath.Clean(TrimSuffix(exe, " (deleted)"))` 后 == `EvalSymlinks(ckmanBin)`。跨软链 / 相对路径启动均稳健，避免把存活进程误判为崩溃而反复拉起。
2. 退化匹配 1：`argv0`（`cmdline[0]`）字面 == ckmanBin 或其规范路径（exe 不可读时兜底）。
3. 退化匹配 2：`basename(argv0)` == `ckman` 且 cmdline 字符串包含本实例 pidfile（原始或规范形式）。**绝不用 conf 子串匹配**（相对 conf 会误命中别的实例）。
4. 跳过 watchdog 自身 PID。

### verdict 状态机

```
findCkmanPids
├─ len==0
│   ├─ pidfile 存在  → CRASH    （崩溃/OOM/kill -9，残留 pidfile）
│   └─ pidfile 不存在 → STOPPED  （go-daemon Release 已删，运维主动停）
├─ len>1             → MULTI     （扫到多个 ckman，异常，仅告警）
└─ len==1 pid
    ├─ procState=="T"                → HUNG  （已停止）
    ├─ procState=="D" 且持续采样确认  → HUNG  （不可中断睡眠，夯死）
    ├─ HTTP 重试后仍无应答            → HUNG  （超时/拒绝）
    ├─ HTTP code != 200              → APP   （接口非健康，仅告警）
    ├─ 依赖深探失败（DB/Nacos）       → DEP   （仅告警）
    └─ 否则                          → HEALTHY
```

### 探测细节

- **procState**：读 `/proc/<pid>/stat` 的 State 字符。`D` 态需连续采样 `DStateSamples`（默认 3）次、间隔 `DStateInterval`（默认 1s）全为 D 才判 HUNG（D 态多为瞬时，避免误判）。
- **HTTP 探活**：`GET <scheme>://127.0.0.1:<port><MetricPath>`，scheme 由 `cfg.Server.Https` 决定（https 时 `InsecureSkipVerify`）。超时 `HttpTimeout`（默认 5s）；无应答（超时/拒绝）局部重试 `HttpRetries`（默认 3）次、间隔 `HttpRetryInterval`（默认 3s）。返回 `(code, ok)`。
  - 若 `cfg.Server.Metric==false`（用户关闭 metrics），探活端点回退为 `/`（嵌入前端，无鉴权且始终 200）。
- **依赖深探（仅自愈模式，仅告警）—— 全部零副作用，绝不 open/迁移 DB**：
  - DB，按 `cfg.Server.PersistentPolicy` 分支：
    - `local`（sqlite/旧版 json）：**完全跳过，不探**。理由：① 旧版本用 `clusters.json`、新版本用 `clusters.db`，stat 固定文件名会把 watchdog 与 ckman 版本绑死，跳过即解耦，watchdog 可作为独立文件跨版本分发；② sqlite/json 是本地文件而非网络服务，没有"远端可达性"可探，真正损坏会以进程崩溃 / HTTP 5xx 暴露（已被 CRASH/APP 覆盖）；③ 绝不 open，避免与运行中的 ckman 争 sqlite 写锁。
    - `mysql`/`postgres`/`dm8`：`net.DialTimeout("tcp", host:port, 3s)`，host/port 取自 `cfg.PersistentConfig[policy]`，端口缺省 3306/5432/5236。连通即 OK（连通性级别探测，零副作用、不鉴权、不迁移）。
  - Nacos：仅当 `cfg.Nacos.Enabled`。对 `cfg.Nacos.Hosts` 逐个 `net.DialTimeout("tcp", host:port, 3s)`，任一可达即视为可用（不启动 SDK，避免注册副作用）。

  **已知局限**：网络库探测仅到连通性层面（端口通 ≠ 库内可用 / 鉴权正确）。因 DEP 只告警、不触发重启，此粒度足够；如需更深，后续可加「不经 `InitPersistent` 的原始只读 `sql.Open`+`Ping`」。

### 自愈动作

- **CRASH** → `runStart()` 执行 `startCmd`（`cmd.Dir=HDIR`，ckman 自守护后存活于 watchdog 退出之后）。
- **HUNG** → `syscall.Kill(pid, SIGKILL)` → `waitGone(pid, 5s)` → `runStart()`。
- **APP / DEP / MULTI** → 仅 `writeAlert("ALERT", ...)`，**不动进程**。
- **STOPPED** → 无动作（不刷心跳）。
- **HEALTHY** → 落一行 `OK` 心跳到 alert 文件（区分「watchdog 未执行」与「静默成功」）。

### 防重启风暴 + 启动宽限

- `run/watchdog.restarts` 记录每次拉起的 unix 时间戳（每行一个）。
- 熔断：滑动窗口 `RestartWindow`（默认 10min）内拉起次数 ≥ `RestartMax`（默认 3）→ 放弃自愈，写 `CRIT` 等待人工。
- 启动宽限：距上次拉起 < `StartupGrace`（默认 60s）→ 本轮直接跳过（避免刚拉起又判异常）。
- 写回时按 `max(window, grace)` 裁剪过期时间戳。

### 单例锁

`run/watchdog.lock` 上 `syscall.Flock(LOCK_EX|LOCK_NB)`；拿不到锁说明上一轮还在跑 → 本轮跳过。

### 日志 / 告警

- watchdog.log：lumberjack 轮转，size/count/age 复用 `cfg.Log`。每轮写 `verdict=... pid=... 证据` 一行。
- ckman-alert.log：`[HEAL]/[ALERT]/[CRIT]/OK` 审计痕迹追加。
- 初始化时把 `log.Logger` 指向 watchdog.log 的 sugar logger，使复用的 `repository`/`nacos` 内部日志也落到 watchdog.log 且不 nil panic。

### `-check` 模式

`-check` flag：仅探测（不做依赖深探，轻量），打印 `LABEL pid=N 证据` 到 stdout + watchdog.log 留痕，按退出码返回，**不自愈、不告警**。退出码：`0` HEALTHY / `10` DOWN(CRASH/STOPPED) / `11` HUNG / `12` APP/MULTI / `3` AUTH(code==401)。供外部监控接入。

## 调参默认值（常量，flag 可覆盖）

| 项 | 默认 |
|----|------|
| HttpTimeout | 5s |
| HttpRetries | 3 |
| HttpRetryInterval | 3s |
| DStateSamples | 3 |
| DStateInterval | 1s |
| RestartWindow | 10min |
| RestartMax | 3 |
| StartupGrace | 60s |
| CheckDB | true（local 策略恒跳过，仅对 mysql/pg/dm8 生效） |
| CheckNacos | true（且 `Nacos.Enabled` 时才真正探） |

## 构建与安装

- Makefile `backend` 目标追加：`CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/watchdog/watchdog cmd/watchdog/watchdog.go`（与 ckmanctl 同款）。
- `package` 目标追加：`cp cmd/watchdog/watchdog → bin/watchdog`。
- 安装文档给 crontab：`* * * * * <HDIR>/bin/watchdog >/dev/null 2>&1`（flock 保证安全）。

## 行为约定（写入文档）

- **想真正停掉 ckman 且不被拉起**：必须用 `bin/stop`（发 SIGTERM，go-daemon 删 pidfile）。`kill -9` 会被当作崩溃而拉起。
- OOM-kill 等同崩溃，会被拉起（符合预期）。
- **版本无关 / 可独立分发**：watchdog 只依赖跨版本稳定的明文配置字段与 OS 级信号（procfs/信号/TCP/HTTP），不触碰 ckman 的 DB schema、不读密码、local 策略不探 DB。因此同一个 watchdog 二进制可投放到不同版本的 ckman tarball 而无需匹配版本。

## 测试策略

- 纯函数单测：`countWithin` / `pruneWithin` / `latest`（重启时间戳窗口逻辑）、`portFromBind`、verdict→退出码映射、`resolveUnderHome`。
- 路径推导 `derivePaths` 在临时目录下用 flag 覆盖验证。
- procfs / kill / HTTP / DB 等带外部副作用的部分以小接口隔离，便于后续注入；首版以纯函数 + 手工验证为主。
