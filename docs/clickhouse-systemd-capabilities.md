# ClickHouse 的 218/CAPABILITIES 启动失败：systemd、内核 backport 与 AmbientCapabilities

## 一、两台版本相近、行为不同的机器

一台机器上，ClickHouse 26.3 启动失败：

```
Failed at step CAPABILITIES spawning /usr/bin/clickhouse-server: Invalid argument
clickhouse-server.service: main process exited, code=exited, status=218/CAPABILITIES
```

另一台机器，同样的安装包、同样的配置文件、同样的 systemd 219，一切正常。

两台机器的内核版本是：

- 失败的那台：`3.10.123`
- 正常的那台：`3.10.0-1160.119.1.el7.x86_64`

从主版本看，两台机器都是 Linux 3.10，systemd 版本也相同，但服务行为不同。

手动试出来的规避方法倒是很直接：把 service 文件里这两行删掉，服务立刻正常：

```ini
CapabilityBoundingSet=CAP_NET_ADMIN CAP_IPC_LOCK CAP_SYS_NICE CAP_NET_BIND_SERVICE
AmbientCapabilities=CAP_NET_ADMIN CAP_IPC_LOCK CAP_SYS_NICE CAP_NET_BIND_SERVICE
```

这能恢复服务，但还需要回答三个问题：为什么会失败，只删其中一行是否足够，以及移除配置会损失哪些能力。

---

## 二、导致失败的是哪一行

两行配置并非都依赖 ambient capabilities：

- `CapabilityBoundingSet=`：capability bounding set 从 Linux 2.6.25（2008 年）起成为每线程属性，systemd 很早就支持该指令。
- `AmbientCapabilities=`：ambient capabilities 在 Linux 4.3（2015-11）引入，对应 `prctl(PR_CAP_AMBIENT, ...)`；systemd 上游从 v229（2016-02）起支持，来自 PR #2265。

当内核没有 `PR_CAP_AMBIENT` 时，systemd 调这个 prctl 会拿到 `EINVAL`，然后原样报出来——`Invalid argument`，正是我们看到的那句。

因此，这个场景只需要清空 `AmbientCapabilities=`，不必同时移除 `CapabilityBoundingSet=`。

这里还藏着一个很有用的诊断信号，可以区分两种完全不同的故障：

| 症状 | 含义 | 后果 |
|---|---|---|
| `Unknown lvalue 'AmbientCapabilities' in section 'Service'` | systemd 不认识这个指令 | 通常记录 warning，并忽略该项 |
| `Failed at step CAPABILITIES ... Invalid argument` | systemd 认识并尝试应用，但内核不支持相应操作 | 启动失败，`218/CAPABILITIES` |

这里遇到的是第二种：当前 systemd 包含相关支持，失败发生在它向内核应用 ambient capabilities 的阶段。

---

## 三、相同主版本不代表相同功能集

两台机器都显示 3.10，但发行版内核通常会在稳定的主版本号下 backport 新特性。

`3.10.0-1160.119.1.el7` 基于上游 3.10，但 Red Hat 在 RHEL 7 生命周期中持续回填修复和新特性。ambient capabilities 就是其中之一：从 RHEL 7.3（`3.10.0-514.el7`）开始，该系列内核提供了这一原本在上游 Linux 4.3 引入的能力。

`3.10.123` 不带常见的发行版标识，而且这个版本号也不属于 kernel.org 发布的上游 3.10.y 序列，更可能来自厂商或设备定制。仅凭 `uname -r` 无法判断它包含哪些 backport；这里需要以运行时能力探测为准。

于是判定方法也很简单，别看 `uname -r`，看内核实际暴露了什么：

```bash
grep CapAmb /proc/self/status
# 有这一行 → 内核支持 ambient capabilities
# 没有     → 当前内核没有暴露 ambient capability 集
```

顺带也确认一下 systemd 侧：

```bash
systemctl show clickhouse-server -p AmbientCapabilities
```

如果能返回该属性，说明当前 systemd 认识这个指令。RHEL 系的 systemd 同样包含大量 backport，不能只用“219 < 229”判断功能是否存在。

### 发行版速查

| 发行版 | systemd | 内核 | 结论 |
|---|---|---|---|
| CentOS/RHEL 7.0–7.2 | 208/219 | `3.10.0-<514` | ❌ 内核缺 |
| CentOS/RHEL 7.3+ | 219（backport） | `3.10.0-514+` | ✅ |
| RHEL/CentOS 8+ | 239+ | 4.18+ | ✅ |
| Ubuntu 16.04+ | 229+ | 4.4+ | ✅ |
| Debian 9+ | 232+ | 4.9+ | ✅ |
| 自编译/定制 3.10.y | 任意 | 上游 3.10 | ❌ |

---

## 四、ClickHouse 何时加入 AmbientCapabilities

接下来要解释为什么旧版本没有出现同样的问题。

从 `packages/clickhouse-server.service` 的 git 历史可以定位配置变化：

| 时间 | commit | 变更 |
|---|---|---|
| 2018-05-17 | `bd4d8a6766e` | 首次出现 `CapabilityBoundingSet=CAP_NET_ADMIN` |
| 2022-02-08 | `342f7ba5671` | 迁移到 nfpm，bounding set 扩到三个 |
| **2023-07-20** | **`1fe8076b942`** | **加入 `AmbientCapabilities=`，两边都补上 `CAP_NET_BIND_SERVICE`** |

最后这个 commit 首次发布于 **`v23.7.1.2470-stable`**。

从 23.7 起，官方 unit 带有 `AmbientCapabilities=`。当 systemd 认识该指令、但内核没有实现 `PR_CAP_AMBIENT` 时，服务会在启动阶段失败；如果 systemd 本身不认识该指令，则通常只会忽略它并记录 warning。

作者 Azat Khuzhin 在提交说明中解释了修改动机：

> `CapabilityBoundingSet` that contained in systemd unit before is about **allowing** to set some capabilities, not about **granting** them. To grant them you need to use `AmbientCapabilities`.
>
> And if you do not use `clickhouse install` then: IO priorities was unavailable (since they require CAP_SYS_NICE); for taskstats the procfs was used instead of netlink. **Not a big deal, but still.**

换句话说，原来的 `CapabilityBoundingSet=` 只限制进程能够获得的能力上限，并不会把这些能力授予以 `clickhouse` 用户运行的进程。

为什么？因为 ClickHouse 跑在 `User=clickhouse` 下。bounding set 是**上限**，不是**授予**：

- 对 root 进程，它会限制可用能力范围；
- 对当前以非 root 用户启动的 ClickHouse，它不会主动授予能力，但仍会约束进程以后通过 `execve` 等路径能够获得的能力。

`AmbientCapabilities=` 用来让非 root 服务在执行普通程序时保留指定能力。ambient set 可以跨非特权 `execve` 保留，并在执行后加入 permitted 和 effective 集，解决了非 root 服务难以把少量能力稳定传递给后续程序的问题。

内核要求 ambient 中的能力同时存在于 permitted 和 inheritable 集中。systemd 会为 `AmbientCapabilities=` 设置相应的能力集合，而 `CapabilityBoundingSet=` 也必须允许这些能力；不过两行不要求内容完全相同，bounding set 可以包含更多能力。

因此，`1fe8076b942` 修复的是实际问题：此前 unit 中列出的能力没有被授予非 root 的 ClickHouse 进程。与此同时，新配置也引入了对 ambient capabilities 内核接口的依赖。

---

## 五、移除这四个能力有什么影响

下面按 ClickHouse 26.3 的代码和默认配置分别检查：

| 能力 | 代码落点 | 换来什么 | 没有会怎样 |
|---|---|---|---|
| `CAP_SYS_NICE` | `src/Common/OSThreadNiceValue.cpp`、`programs/server/Server.cpp:3429` | 支持负 nice 值及部分 IO priority 操作 | 相关优先级设置无法生效，并可能记录提示日志 |
| `CAP_IPC_LOCK` | `programs/server/Server.cpp:1306` | `mlock_executable` 把代码段锁进物理内存 | 启用该设置时无法完成锁定；默认值为 `false` |
| `CAP_NET_BIND_SERVICE` | 内核端口权限检查 | 允许非 root 用户绑定 1024 以下端口 | 使用低端口时绑定失败；默认的 8123/9000/9440 不受影响 |
| `CAP_NET_ADMIN` | 见下节 | （曾经是 netlink taskstats） | 见下节 |

对使用默认端口、未启用 `mlock_executable`、未配置依赖 `CAP_SYS_NICE` 的线程或 IO 优先级功能的实例，这些能力不影响基本查询和存储功能。但如果使用了上述配置，不能把它们简单视为“可选优化”。

---

## 六、26.3 已不再使用 CAP_NET_ADMIN 采集 taskstats

`CAP_NET_ADMIN` 的原始用途是通过 **netlink taskstats** 采集线程级 OS 指标，喂给 `ProfileEvents` 里的 `OSIOWaitMicroseconds`（真实 IO 等待，不含 page cache）、`OSCPUWaitMicroseconds`（就绪但未被调度的时间）、`OSReadChars` 等。没有它就降级到 `ProcfsMetricsProvider`，改读 `/proc/<tid>/{stat,io}`。

这是 23.7 时代的实现。ClickHouse 在 2026 年 2 月提交了以下修改：

```
b503accc7f9  2026-02-28  Remove NetlinkMetricsProvider, use procfs exclusively for taskstats
```

在 26.3 的源码树里，`src/Common/` 下已经**没有** `NetlinkMetricsProvider` 这个文件了。`findBestAvailableProvider()` 现在只在两个选项间挑：

```cpp
static std::optional<MetricsProvider> provider =
    []() -> MetricsProvider
    {
        if (ProcfsMetricsProvider::isAvailable())
            return MetricsProvider::Procfs;
        return MetricsProvider::None;
    }();
```

该提交移除了 `NetlinkMetricsProvider`，taskstats 统一从 procfs 获取。因此，26.3 的这条指标采集路径不再需要 unit 中的 `CAP_NET_ADMIN`。

在 26.3 上，`hasLinuxCapability()` 的全部调用方只剩：

```
src/Common/OSThreadNiceValue.cpp:22     → CAP_SYS_NICE
programs/server/Server.cpp:1311         → CAP_IPC_LOCK  (mlock)
programs/server/Server.cpp:3429         → CAP_SYS_NICE  (启动提示)
src/Common/ThreadProfileEvents.cpp:418  → CAP_SYS_ADMIN (perf events)
```

最后一个 `CAP_SYS_ADMIN` 不在该 unit 的 ambient capabilities 列表中，因此这个 unit 不会通过该配置向 ClickHouse 授予 perf events 所需的能力。

对 ClickHouse 26.3，如果实例使用默认端口、没有启用 `mlock_executable`，也没有配置依赖 `CAP_SYS_NICE` 的线程或 IO 优先级功能，清空 `AmbientCapabilities=` 不影响基本服务功能。

这里也能看到打包配置和代码演进不同步的问题：unit 中的能力列表来自 2023 年的实现，netlink 代码后来被移除，相关配置却仍然保留。

---

## 七、为什么 clickhouse-keeper 没有同样失败

同一台机器上的 `clickhouse-keeper` 可以正常启动。

打开 `packages/clickhouse-keeper.service`：

```ini
User=clickhouse
Group=clickhouse
...
CapabilityBoundingSet=CAP_NET_ADMIN CAP_IPC_LOCK CAP_SYS_NICE CAP_NET_BIND_SERVICE
```

该 unit 只有 bounding set，没有 `AmbientCapabilities=`。相关 git 历史只有两条：

```
6537029cccd 2023-03-02 fix: keeper systemd service file include invalid inline comment
26a292dafcb 2023-01-24 Add systemd service-file for clickhouse-keeper
```

2023-07 的 server 修改没有同步修改 keeper。仅从提交记录无法判断这是有意保留还是遗漏。

从 keeper 的入口代码和配置项中，没有发现对这四项能力的直接依赖：

```bash
grep -rn "mlock\|hasLinuxCapability\|CAP_\|ioprio\|NiceValue" programs/keeper/*.cpp programs/keeper/*.h
# 无输出
```

逐条核对：

| 能力 | keeper 的情况 |
|---|---|
| `CAP_IPC_LOCK` | `mlock_executable` 属于 `ServerSettings`；keeper 使用 `CoordinationSettings`，没有对应配置项 |
| `CAP_SYS_NICE` | `os_thread_priority` 同属 Server 侧，`CoordinationSettings.cpp` 里没有任何 nice/priority 项 |
| `CAP_NET_BIND_SERVICE` | 默认端口 9181（客户端）/ 9234（raft），都远在 1024 以上 |
| `CAP_NET_ADMIN` | 通用 taskstats 路径在 26.3 中已改用 procfs |

`Keeper.cpp:351` 中的 `MainThreadStatus::getInstance()` 会进入通用 taskstats 路径，而 26.3 的这条路径只使用 procfs，不需要 `CAP_NET_ADMIN`。上述源码检查能够说明当前已知执行路径没有直接需求，但不能仅凭对 `programs/keeper` 的 grep 排除所有传递依赖。

因此，不应只为与 server unit 保持形式一致而给 keeper 增加 `AmbientCapabilities=`：

1. 当前没有找到需要授予这些能力的具体功能；
2. 增加后，会让 keeper 在 systemd 支持该指令但内核缺少 `PR_CAP_AMBIENT` 的环境中遇到同类启动失败；
3. 如果以后 keeper 需要绑定低端口或新增 mlock、priority 等功能，应根据实际需求配置最小能力集。

keeper 现有的 `CapabilityBoundingSet=` 仍有安全边界作用。例如 unit 如果以后改为 root 用户运行，它会限制进程可用的能力范围，因此没有必要随 ambient 配置一起删除。

---

## 八、使用 systemd drop-in 修复

不要直接修改 vendor unit。`/usr/lib/systemd/system/clickhouse-server.service` 可能被 RPM 升级覆盖，应使用 drop-in 保留本地配置。

用 drop-in：

```bash
mkdir -p /etc/systemd/system/clickhouse-server.service.d
cat > /etc/systemd/system/clickhouse-server.service.d/no-ambient-caps.conf <<'EOF'
[Service]
# 本机内核无 PR_CAP_AMBIENT（上游 Linux 4.3+ 才有，RHEL 7.3/3.10.0-514 起 backport），
# 应用 ambient capabilities 时 prctl 返回 EINVAL，导致 218/CAPABILITIES。
# 空赋值用于重置该指令，CapabilityBoundingSet 保留。
AmbientCapabilities=
EOF

systemctl daemon-reload && systemctl restart clickhouse-server
```

systemd 对列表型指令的空赋值表示重置。这个 drop-in 只清空 ambient capabilities，保留 bounding set，并且不会因普通的软件包升级而被覆盖。

ckman 在 RPM/DEB 部署的安装、升级、启动和重启阶段会自动探测运行时能力。其判断逻辑等价于：

```bash
grep -q '^CapAmb:' /proc/self/status || <下发上面的 drop-in>
```

缺少 `CapAmb` 时，ckman 创建 `/etc/systemd/system/clickhouse-server.service.d/ckman-no-ambient-capabilities.conf`；内核支持该能力时，则删除这个由 ckman 管理的 drop-in。这样既能避免旧内核节点出现 `218/CAPABILITIES`，也能在节点升级内核后恢复官方 unit 的能力配置。`CapAmb` 的值是否为零并不影响判断，只要字段存在就说明内核提供了 ambient capabilities。

---

## 九、结论

这个案例说明，发行版大量 backport 特性后，内核和 systemd 的主版本号不足以代表实际功能集。排查时应结合 `/proc/self/status`、systemd 属性和具体失败阶段，而不是只比较版本号。

清空 `AmbientCapabilities=` 可以解决本文环境中的启动问题，但是否影响业务取决于实例配置。使用低端口、`mlock_executable`、线程或 IO 优先级功能时，需要单独评估并选择升级内核、调整配置或通过其他方式授予最小能力集。对 vendor unit 的本地调整应通过 drop-in 完成，避免被软件包升级覆盖。

---

## 参考

- [ClickHouse commit `1fe8076b942` — Fix capabilities installed via systemd service (fixes netlink/IO priorities)](https://github.com/ClickHouse/ClickHouse/commit/1fe8076b942)
- [ClickHouse commit `b503accc7f9` — Remove NetlinkMetricsProvider, use procfs exclusively for taskstats](https://github.com/ClickHouse/ClickHouse/commit/b503accc7f9)
- [systemd v229 released — LWN](https://lwn.net/Articles/676160/)
- [systemd.exec(5) — `AmbientCapabilities=`, "Added in version 229"](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)
- [systemd PR #2265 — capabilities: added support for ambient capabilities](https://github.com/systemd/systemd/pull/2265)
- [scylladb#3176 — Capabilities error during service start on CentOS with older kernel](https://github.com/scylladb/scylladb/issues/3176)（RHEL 7 内核 `3.10.0-514` 分界线的出处）
- [systemd#5000 — Clarify CAPABILITIES exec error "Invalid argument"](https://github.com/systemd/systemd/issues/5000)
- [Does RHEL support Ambient Capabilities? — Red Hat Solutions](https://access.redhat.com/solutions/1264083)（需登录）
- [capabilities(7) — Linux capability sets and execve rules](https://man7.org/linux/man-pages/man7/capabilities.7.html)
- [Linux kernel 3.x archive](https://www.kernel.org/pub/linux/kernel/v3.x/)
