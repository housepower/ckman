# ckman watchdog 自愈守护

`bin/watchdog` 是 ckman 的自愈守护程序，由 crontab 每分钟调起、一次性运行。

## 行为

| 状态 | 判定 | 动作 |
|------|------|------|
| CRASH | 进程不在 + pidfile 残留 | 拉起(`bin/start`) |
| STOPPED | 进程不在 + 无 pidfile | **不干预**(运维主动停) |
| HUNG | 持续 D 态 / T 态 / 接口无应答 | `kill -9` + 拉起 |
| APP | 接口返回非 200 | 仅告警 |
| DEP | DB/Nacos 不可达 | 仅告警 |
| MULTI | 扫到多个 ckman 进程 | 仅告警 |
| HEALTHY | 接口 200 且依赖正常 | 写心跳 |

- 真相源是「进程是否存在」：用 procfs 按 `/proc/<pid>/exe` 二进制身份扫描（跨软链稳健），pidfile 仅在进程缺失时区分崩溃(残留)与主动停(go-daemon 优雅退出已删)。
- 探活端点：`/metrics`(无鉴权，默认开启)；关闭 metric 时回退 `/`。
- 依赖探测零副作用：`local` 策略不探 DB；`mysql/postgres/dm8` 与 Nacos 仅 TCP 拨号；不读密码、不连库迁移。故同一 watchdog 二进制可跨 ckman 版本独立分发。
- 防重启风暴：10 分钟内最多重启 3 次，超限熔断只告警；距上次拉起 60s 内跳过(启动宽限)。

## 重要约定

- **想真正停掉 ckman 且不被拉起**：必须用 `bin/stop`(发 SIGTERM，go-daemon 删 pidfile)。直接 `kill -9` 会被当作崩溃而拉起。
- OOM-kill 等同崩溃，会被拉起。

## crontab 安装

```cron
* * * * * /path/to/ckman/bin/watchdog >/dev/null 2>&1
```

`flock`(`run/watchdog.lock`)保证不会叠跑。

## 日志

- `logs/watchdog.log`：每轮探测的 verdict 与动作。
- `logs/ckman-alert.log`：`[HEAL]/[ALERT]/[CRIT]/OK` 审计痕迹。

## 手动探测(接入外部监控)

```bash
bin/watchdog -check    # 仅探测,不自愈;退出码 0=健康 10=DOWN 11=HUNG 12=APP/异常 3=鉴权
```

## flag

- `-c <path>`：配置文件路径(默认 `<HDIR>/conf/ckman.hjson`)
- `-pid <path>`：pidfile 路径(默认 `<HDIR>/run/ckman.pid`)
- `-check`：仅探测模式

## 路径推导

watchdog 照 `bin/start` 风格自推导：以二进制自身位置 `bin/watchdog` 的上级目录为 HDIR，据此定位 `conf/ckman.hjson`、`run/ckman.pid`、`logs/`、`run/watchdog.{lock,restarts}`。可用 `-c` / `-pid` 覆盖。
