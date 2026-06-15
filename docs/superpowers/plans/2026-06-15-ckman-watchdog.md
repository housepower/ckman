# ckman watchdog 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 tarball 部署的 ckman 实现一个由 crontab 调起、一次性运行的自愈守护程序 `cmd/watchdog/watchdog.go`，进程缺失→拉起、夯死→kill+重启、依赖不可用→仅告警，并防重启风暴。

**Architecture:** 单文件 `package main`，复用 ckman 的 `config`/`log` 包读取现有 `ckman.hjson`（仅明文稳定字段，不解密、不连库）。真相源是 procfs 按二进制身份扫描进程，pidfile 仅在进程缺失时区分崩溃(残留)与主动停(已删)。依赖探测全部零副作用（local 跳过、网络库与 nacos 只 TCP 拨号）。

**Tech Stack:** Go 1.24；`github.com/prometheus/procfs`、`go.uber.org/zap`、`gopkg.in/natefinch/lumberjack.v2`（均已在 go.mod）；标准库 `net/http`、`net`、`syscall`、`os/exec`。

**对应设计：** `docs/superpowers/specs/2026-06-15-ckman-watchdog-design.md`

---

## File Structure

- **Create** `cmd/watchdog/watchdog.go` — watchdog 全部逻辑（单文件 main，约 500 行）。
- **Create** `cmd/watchdog/watchdog_test.go` — 纯函数单测（窗口逻辑、db 地址提取、退出码映射、探活路径选择、路径推导）。
- **Modify** `Makefile` — `backend`/`debug` 增加 watchdog 构建；`package` 增加拷贝 `bin/watchdog`。
- **Create** `docs/watchdog.md` — 部署/crontab/行为约定文档。

命名约定（全计划统一，后续任务依赖）：

- 类型：`verdict int`，常量 `vHealthy/vCrash/vStopped/vHung/vApp/vDep/vMulti`；`probeResult{v verdict; pid int; httpCode int; evidence string}`；`paths{hdir,conf,pidfile,ckmanBin,startCmd,runDir,watchdogLog,alertLog,restartsFile,lockFile string}`。
- 调参为包级 `var`（带默认值，无 flag，YAGNI）：`httpTimeout=5s`、`httpRetries=3`、`httpRetryInterval=3s`、`dStateSamples=3`、`dStateInterval=1s`、`restartWindow=10m`、`restartMax=3`、`startupGrace=60s`、`checkDB=true`、`checkNacos=true`。
- flag：仅 `-c`(conf)、`-pid`、`-check`。
- 包级 `var alertWriter *lumberjack.Logger`。

---

## Task 1: 脚手架 — 类型、flag、路径推导、main 骨架（可编译）

**Files:**
- Create: `cmd/watchdog/watchdog.go`

- [ ] **Step 1: 写入文件骨架**

```go
// watchdog 是 ckman 的自愈守护程序：一次性运行，由 crontab 每分钟调起。
//
// 真相源是"ckman 进程是否存在"（procfs 按二进制身份扫描），pidfile 仅在
// 进程不存在时用来区分"崩溃"(残留) 与"运维主动停"(go-daemon 优雅退出已删)。
//
//	进程在    → 探状态(D/T)/接口/依赖
//	进程不在  → pidfile 在=崩溃→拉起；pidfile 不在=主动停→idle
//
// 自愈：崩溃→拉起；夯死(持续 D/T / 接口无应答)→kill -9 + 拉起；
// 接口非健康/依赖(DB·Nacos)挂→仅告警。所有动作写 [HEAL] 审计痕迹。
//
// 零配置：只读现有 ckman.hjson 的明文稳定字段，不解密、不连库；local 持久化
// 策略不探 DB，故同一二进制可跨 ckman 版本独立分发。
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/procfs"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
)

// Version 由 ldflags 注入（与 ckman 主程序同款），传给 ParseConfigFile。
var Version = "watchdog"

var (
	confFlag  = flag.String("c", "", "配置文件路径(默认 <HDIR>/conf/ckman.hjson)")
	pidFlag   = flag.String("pid", "", "覆盖 pidfile 路径(默认 <HDIR>/run/ckman.pid)")
	checkFlag = flag.Bool("check", false, "仅探测：打印状态并按退出码返回，不自愈、不告警(会往 watchdog.log 留一行痕迹)")
)

// 调参默认值（包级 var 便于测试覆盖；零配置，无 flag）。
var (
	httpTimeout       = 5 * time.Second
	httpRetries       = 3
	httpRetryInterval = 3 * time.Second
	dStateSamples     = 3
	dStateInterval    = 1 * time.Second
	restartWindow     = 10 * time.Minute
	restartMax        = 3
	startupGrace      = 60 * time.Second
	checkDB           = true
	checkNacos        = true
)

type verdict int

const (
	vHealthy verdict = iota
	vCrash
	vStopped
	vHung
	vApp
	vDep
	vMulti
)

func (v verdict) String() string {
	switch v {
	case vHealthy:
		return "HEALTHY"
	case vCrash:
		return "CRASH"
	case vStopped:
		return "STOPPED"
	case vHung:
		return "HUNG"
	case vApp:
		return "APP"
	case vDep:
		return "DEP"
	case vMulti:
		return "MULTI"
	}
	return "UNKNOWN"
}

type probeResult struct {
	v        verdict
	pid      int
	httpCode int    // 接口层得到的 HTTP 状态码(0 表示无应答)
	evidence string // 人类可读证据
}

type paths struct {
	hdir         string
	conf         string
	pidfile      string
	ckmanBin     string
	startCmd     string
	runDir       string
	watchdogLog  string
	alertLog     string
	restartsFile string
	lockFile     string
}

var alertWriter *lumberjack.Logger

func main() {
	flag.Parse()

	p := derivePaths()

	if err := config.ParseConfigFile(p.conf, Version); err != nil {
		fmt.Fprintf(os.Stderr, "watchdog: 解析配置失败 %s: %v\n", p.conf, err)
		os.Exit(3)
	}
	cfg := &config.GlobalConfig

	if *checkFlag {
		runCheck(cfg, &p)
		return
	}
	initLogger(cfg, &p)
	runHeal(cfg, &p)
}

// derivePaths 照 bin/start 风格自推导：DIR=二进制目录, HDIR=其上级。
func derivePaths() paths {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	dir := filepath.Dir(exe)
	hdir := filepath.Dir(dir)
	p := paths{
		hdir:     hdir,
		conf:     filepath.Join(hdir, "conf", "ckman.hjson"),
		pidfile:  filepath.Join(hdir, "run", "ckman.pid"),
		ckmanBin: filepath.Join(hdir, "bin", "ckman"),
		runDir:   filepath.Join(hdir, "run"),
	}
	if *confFlag != "" {
		p.conf = *confFlag
	}
	if *pidFlag != "" {
		if abs, err := filepath.Abs(*pidFlag); err == nil {
			p.pidfile = abs
		} else {
			p.pidfile = *pidFlag
		}
	}
	p.startCmd = filepath.Join(hdir, "bin", "start")
	p.watchdogLog = filepath.Join(hdir, "logs", "watchdog.log")
	p.alertLog = filepath.Join(hdir, "logs", "ckman-alert.log")
	p.restartsFile = filepath.Join(p.runDir, "watchdog.restarts")
	p.lockFile = filepath.Join(p.runDir, "watchdog.lock")
	return p
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
```

> 注：本步引用了尚未定义的 `runCheck`/`initLogger`/`runHeal`，后续任务补齐。为先让其编译，**临时**在文件末尾加三个空桩：

```go
func runCheck(cfg *config.CKManConfig, p *paths) {}
func initLogger(cfg *config.CKManConfig, p *paths) {}
func runHeal(cfg *config.CKManConfig, p *paths) {}
```

- [ ] **Step 2: 编译验证**

Run: `cd /data/root/go/src/github.com/housepower/ckman && go build ./cmd/watchdog/`
Expected: 报 "imported and not used"（Task 1 仅用到 flag/fmt/os/filepath/time/config/lumberjack，其余 9 个 import 尚未使用）。按下一步消除。

> **临时**在 `var alertWriter` 那一行下方加入下面 9 行独立占位（覆盖全部尚未使用的 import；每行都是完整声明，后续任务用到该包时整行删除，无残留）：
> ```go
> var _ = tls.Config{}      // 占位,Task 4 删
> var _ = net.Dial          // 占位,Task 5 删
> var _ = http.MethodGet    // 占位,Task 4 删
> var _ = exec.Command      // 占位,Task 8 删
> var _ = strconv.Itoa      // 占位,Task 2 删
> var _ = strings.TrimSpace // 占位,Task 2 删
> var _ = syscall.Kill      // 占位,Task 8 删
> var _ = procfs.AllProcs   // 占位,Task 3 删
> var _ = log.Logger        // 占位,Task 7 删
> ```
> 再次 `go build ./cmd/watchdog/`，预期编译通过。

- [ ] **Step 3: 提交**

```bash
cd /data/root/go/src/github.com/housepower/ckman
git add cmd/watchdog/watchdog.go
git commit -m "feat(watchdog): 脚手架(类型/flag/路径推导/main 骨架)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: 重启时间戳状态文件 + 窗口逻辑（TDD）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`
- Test: `cmd/watchdog/watchdog_test.go`

- [ ] **Step 1: 写失败测试**

创建 `cmd/watchdog/watchdog_test.go`：

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRestartsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "watchdog.restarts")
	now := time.Now()
	in := []time.Time{now.Add(-2 * time.Hour), now.Add(-30 * time.Minute), now}
	writeRestarts(f, in)
	out := readRestarts(f)
	if len(out) != 3 {
		t.Fatalf("want 3 timestamps, got %d", len(out))
	}
	// 秒级精度往返
	if out[2].Unix() != now.Unix() {
		t.Errorf("last ts mismatch: %d vs %d", out[2].Unix(), now.Unix())
	}
}

func TestReadRestartsMissingFile(t *testing.T) {
	if got := readRestarts(filepath.Join(t.TempDir(), "nope")); got != nil {
		t.Errorf("missing file should yield nil, got %v", got)
	}
}

func TestCountWithin(t *testing.T) {
	now := time.Now()
	ts := []time.Time{now.Add(-20 * time.Minute), now.Add(-5 * time.Minute), now.Add(-1 * time.Minute)}
	if n := countWithin(ts, 10*time.Minute); n != 2 {
		t.Errorf("want 2 within 10m, got %d", n)
	}
}

func TestPruneWithin(t *testing.T) {
	now := time.Now()
	ts := []time.Time{now.Add(-2 * time.Hour), now.Add(-1 * time.Minute)}
	got := pruneWithin(ts, time.Hour)
	if len(got) != 1 {
		t.Fatalf("want 1 after prune, got %d", len(got))
	}
}

func TestLatest(t *testing.T) {
	now := time.Now()
	ts := []time.Time{now.Add(-time.Hour), now, now.Add(-time.Minute)}
	if !latest(ts).Equal(now) {
		t.Errorf("latest wrong: %v", latest(ts))
	}
	if !latest(nil).IsZero() {
		t.Errorf("latest(nil) should be zero")
	}
}

// 确保 os 包在测试中被使用（避免后续编辑误删 import）
var _ = os.Stat
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `cd /data/root/go/src/github.com/housepower/ckman && go test ./cmd/watchdog/ -run TestRestarts -v`
Expected: 编译失败 / `undefined: writeRestarts` 等。

- [ ] **Step 3: 实现窗口逻辑**

在 `cmd/watchdog/watchdog.go` 末尾追加：

```go
// ---------------- 状态文件(重启时间戳) ----------------

func readRestarts(path string) []time.Time {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var ts []time.Time
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if sec, err := strconv.ParseInt(line, 10, 64); err == nil {
			ts = append(ts, time.Unix(sec, 0))
		}
	}
	return ts
}

func writeRestarts(path string, ts []time.Time) {
	var b strings.Builder
	for _, t := range ts {
		b.WriteString(strconv.FormatInt(t.Unix(), 10))
		b.WriteByte('\n')
	}
	_ = os.WriteFile(path, []byte(b.String()), 0644)
}

func countWithin(ts []time.Time, window time.Duration) int {
	cut := time.Now().Add(-window)
	n := 0
	for _, t := range ts {
		if t.After(cut) {
			n++
		}
	}
	return n
}

func pruneWithin(ts []time.Time, window time.Duration) []time.Time {
	cut := time.Now().Add(-window)
	out := make([]time.Time, 0, len(ts))
	for _, t := range ts {
		if t.After(cut) {
			out = append(out, t)
		}
	}
	return out
}

func latest(ts []time.Time) time.Time {
	var newest time.Time
	for _, t := range ts {
		if t.After(newest) {
			newest = t
		}
	}
	return newest
}
```

> 此时 `strconv` 与 `strings` 已被真实使用，删除 Task 1 里 `var _ = strconv.Itoa` 和 `var _ = strings.TrimSpace` 两行占位。

- [ ] **Step 4: 运行测试，确认通过**

Run: `cd /data/root/go/src/github.com/housepower/ckman && go test ./cmd/watchdog/ -run 'TestRestarts|TestReadRestarts|TestCountWithin|TestPruneWithin|TestLatest' -v`
Expected: PASS（5 个测试全过）。

- [ ] **Step 5: 提交**

```bash
git add cmd/watchdog/watchdog.go cmd/watchdog/watchdog_test.go
git commit -m "feat(watchdog): 重启时间戳状态文件与滑动窗口逻辑(TDD)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: 进程身份扫描 procfs（findCkmanPids / procState / persistentD）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`
- Test: `cmd/watchdog/watchdog_test.go`

- [ ] **Step 1: 实现进程扫描**

在 `cmd/watchdog/watchdog.go` 追加：

```go
// ---------------- 进程身份扫描 ----------------

// findCkmanPids 用 procfs 按身份扫描 ckman 进程(不读 pidfile 判存活)。
//
// 主匹配走 /proc/<pid>/exe 的内核规范路径(软链已消解)，跨软链/相对路径启动稳健，
// 避免把存活进程误判为崩溃→反复拉起→重启风暴。
func findCkmanPids(p paths) []int {
	procs, err := procfs.AllProcs()
	if err != nil {
		return nil
	}
	self := os.Getpid()
	base := filepath.Base(p.ckmanBin)
	ckmanBin := filepath.Clean(p.ckmanBin)
	realCkmanBin := ckmanBin
	if r, err := filepath.EvalSymlinks(ckmanBin); err == nil {
		realCkmanBin = r
	}
	realPidfile := p.pidfile
	if p.pidfile != "" {
		if r, err := filepath.EvalSymlinks(p.pidfile); err == nil {
			realPidfile = r
		}
	}
	var pids []int
	for _, pr := range procs {
		if pr.PID == self {
			continue
		}
		// 主匹配：/proc/<pid>/exe 规范绝对路径(软链消解)；二进制被替换时内核标 " (deleted)"。
		if exe, err := pr.Executable(); err == nil && exe != "" {
			if filepath.Clean(strings.TrimSuffix(exe, " (deleted)")) == realCkmanBin {
				pids = append(pids, pr.PID)
				continue
			}
		}
		cmd, err := pr.CmdLine()
		if err != nil || len(cmd) == 0 {
			continue // 僵尸进程 cmdline 为空，自然跳过
		}
		argv0 := filepath.Clean(cmd[0])
		// 退化匹配 1：argv0 字面即规范二进制路径(exe 不可读时兜底)
		match := argv0 == ckmanBin || argv0 == realCkmanBin
		// 退化匹配 2：同名 且 cmdline 引用本实例 pidfile(绝不用 conf 子串，相对 conf 会误命中别的实例)
		if !match && filepath.Base(argv0) == base {
			joined := strings.Join(cmd, " ")
			if (p.pidfile != "" && strings.Contains(joined, p.pidfile)) ||
				(realPidfile != "" && strings.Contains(joined, realPidfile)) {
				match = true
			}
		}
		if match {
			pids = append(pids, pr.PID)
		}
	}
	return pids
}

// procState 取 /proc/<pid>/stat 的状态字符(R/S/D/Z/T...)。
func procState(pid int) string {
	pr, err := procfs.NewProc(pid)
	if err != nil {
		return ""
	}
	st, err := pr.Stat()
	if err != nil {
		return ""
	}
	return st.State
}

// persistentD 采样确认进程是否"持续 D"(全部采样均为 D 才算)。
func persistentD(pid int) bool {
	n := dStateSamples
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		if procState(pid) != "D" {
			return false
		}
		if i < n-1 {
			time.Sleep(dStateInterval)
		}
	}
	return true
}
```

> 此时 `procfs` 已被真实使用，删除 Task 1 的 `var _ = procfs.AllProcs` 占位。

- [ ] **Step 2: 加自检测试（扫描当前测试进程）**

在 `watchdog_test.go` 追加：

```go
func TestFindCkmanPidsMatchesSelfBinary(t *testing.T) {
	// 用测试二进制自身路径冒充 ckmanBin：当前进程应被扫到。
	exe, err := os.Executable()
	if err != nil {
		t.Skip("cannot resolve self exe")
	}
	p := paths{ckmanBin: exe}
	pids := findCkmanPids(p)
	// findCkmanPids 跳过自身 PID(os.Getpid)，故测试进程本身不会出现；
	// 这里只验证不 panic 且返回切片(可能为空)。
	_ = pids
}

func TestProcStateSelf(t *testing.T) {
	// 当前进程一定可读到状态字符(R/S 等)，且非空。
	if s := procState(os.Getpid()); s == "" {
		t.Errorf("procState(self) returned empty")
	}
}
```

- [ ] **Step 3: 运行测试**

Run: `cd /data/root/go/src/github.com/housepower/ckman && go test ./cmd/watchdog/ -run 'TestFindCkmanPids|TestProcState' -v`
Expected: PASS（procfs 在 Linux 下可读 /proc）。

- [ ] **Step 4: 提交**

```bash
git add cmd/watchdog/watchdog.go cmd/watchdog/watchdog_test.go
git commit -m "feat(watchdog): procfs 按二进制身份扫描进程(跨软链稳健)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: HTTP 探活（probePath / httpProbeOnce / httpProbeWithRetry / portFromAddr）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`
- Test: `cmd/watchdog/watchdog_test.go`

- [ ] **Step 1: 写 probePath 失败测试**

在 `watchdog_test.go` 追加：

```go
func TestProbePath(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.Metric = true
	cfg.Server.MetricPath = "/metrics"
	if got := probePath(cfg); got != "/metrics" {
		t.Errorf("metric on: want /metrics, got %s", got)
	}
	cfg.Server.Metric = false
	if got := probePath(cfg); got != "/" {
		t.Errorf("metric off: want /, got %s", got)
	}
	cfg.Server.Metric = true
	cfg.Server.MetricPath = ""
	if got := probePath(cfg); got != "/metrics" {
		t.Errorf("empty metric_path: want /metrics fallback, got %s", got)
	}
}
```

> 测试文件顶部 import 需含 `"github.com/housepower/ckman/config"`，若缺则补上。

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./cmd/watchdog/ -run TestProbePath -v`
Expected: `undefined: probePath`。

- [ ] **Step 3: 实现 HTTP 探活**

在 `cmd/watchdog/watchdog.go` 追加：

```go
// ---------------- HTTP 探活 ----------------

// probePath 选探活端点：默认 /metrics(无鉴权、默认开启)；关了 metric 则回退 / (嵌入前端，始终 200)。
func probePath(cfg *config.CKManConfig) string {
	if !cfg.Server.Metric {
		return "/"
	}
	if mp := strings.TrimSpace(cfg.Server.MetricPath); mp != "" {
		return mp
	}
	return "/metrics"
}

// httpProbeWithRetry 探接口；无应答(超时/拒绝)时局部重试 httpRetries 次。
// 返回 (HTTP 状态码, 是否拿到应答)。
func httpProbeWithRetry(cfg *config.CKManConfig) (int, bool) {
	if code, ok := httpProbeOnce(cfg); ok {
		return code, true
	}
	for i := 0; i < httpRetries; i++ {
		time.Sleep(httpRetryInterval)
		if code, ok := httpProbeOnce(cfg); ok {
			return code, true
		}
	}
	return 0, false
}

func httpProbeOnce(cfg *config.CKManConfig) (int, bool) {
	scheme := "http"
	if cfg.Server.Https {
		scheme = "https"
	}
	url := fmt.Sprintf("%s://127.0.0.1:%d%s", scheme, cfg.Server.Port, probePath(cfg))
	client := &http.Client{
		Timeout:   httpTimeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, false
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false // 超时或拒绝，统一视为"无应答"
	}
	defer resp.Body.Close()
	return resp.StatusCode, true
}

// portFromAddr 从 "host:port" 取端口字符串，取不到返回空。
func portFromAddr(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 && i < len(addr)-1 {
		return addr[i+1:]
	}
	return ""
}
```

> `http`/`tls` 已被真实使用，删除 Task 1 对应 `var _` 占位。`portFromAddr` 供后续 nacos/db 复用。

- [ ] **Step 4: 运行测试**

Run: `go test ./cmd/watchdog/ -run TestProbePath -v`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add cmd/watchdog/watchdog.go cmd/watchdog/watchdog_test.go
git commit -m "feat(watchdog): HTTP 探活(/metrics 无鉴权,关则回退 /;带重试)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: 依赖探测（local 跳过 / 网络库 + nacos TCP 拨号）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`
- Test: `cmd/watchdog/watchdog_test.go`

- [ ] **Step 1: 写 dbAddr 失败测试**

在 `watchdog_test.go` 追加：

```go
func TestDBAddrLocalSkipped(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "local"
	if _, ok := dbAddr(cfg); ok {
		t.Errorf("local policy must be skipped (ok=false)")
	}
}

func TestDBAddrMysqlDefaults(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "mysql"
	cfg.PersistentConfig = map[string]map[string]interface{}{
		"mysql": {"host": "10.0.0.5"}, // 缺 port → 取默认 3306
	}
	addr, ok := dbAddr(cfg)
	if !ok || addr != "10.0.0.5:3306" {
		t.Errorf("want 10.0.0.5:3306 ok=true, got %q ok=%v", addr, ok)
	}
}

func TestDBAddrPostgresExplicitPort(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "postgres"
	cfg.PersistentConfig = map[string]map[string]interface{}{
		"postgres": {"host": "db1", "port": float64(6543)}, // hjson 数字常解析为 float64
	}
	addr, ok := dbAddr(cfg)
	if !ok || addr != "db1:6543" {
		t.Errorf("want db1:6543, got %q ok=%v", addr, ok)
	}
}

func TestDBAddrMissingHost(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "dm8"
	cfg.PersistentConfig = map[string]map[string]interface{}{"dm8": {}}
	if _, ok := dbAddr(cfg); ok {
		t.Errorf("missing host must yield ok=false")
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./cmd/watchdog/ -run TestDBAddr -v`
Expected: `undefined: dbAddr`。

- [ ] **Step 3: 实现依赖探测**

在 `cmd/watchdog/watchdog.go` 追加：

```go
// ---------------- 依赖探测(零副作用,仅告警) ----------------

// 网络库默认端口。local 不在此表 → 不探。
var dbDefaultPort = map[string]int{
	"mysql":    3306,
	"postgres": 5432,
	"dm8":      5236,
}

// pcInt 从 PersistentConfig 子 map 取整型(兼容 hjson 的 float64/int/string)。
func pcInt(m map[string]interface{}, key string) (int, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(n)); err == nil {
			return i, true
		}
	}
	return 0, false
}

func pcString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

// dbAddr 返回网络库的 host:port。local(或未知/缺 host)返回 ok=false 表示"不探"。
func dbAddr(cfg *config.CKManConfig) (string, bool) {
	policy := strings.ToLower(strings.TrimSpace(cfg.Server.PersistentPolicy))
	defPort, networked := dbDefaultPort[policy]
	if !networked {
		return "", false // local / 空 / 未知 → 跳过
	}
	m := cfg.PersistentConfig[policy]
	host := pcString(m, "host")
	if host == "" {
		return "", false
	}
	port := defPort
	if pp, ok := pcInt(m, "port"); ok && pp > 0 {
		port = pp
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), true
}

// depCheck 探 DB / Nacos(仅告警)。返回失败描述，空串表示都正常(或都跳过)。
func depCheck(cfg *config.CKManConfig) string {
	var msgs []string
	if checkDB {
		if addr, ok := dbAddr(cfg); ok {
			if err := dialTCP(addr); err != nil {
				msgs = append(msgs, fmt.Sprintf("DB(%s) 探测失败: %v", addr, err))
			}
		}
	}
	if checkNacos && cfg.Nacos.Enabled {
		if err := checkNacosDep(cfg); err != nil {
			msgs = append(msgs, "Nacos 探测失败: "+err.Error())
		}
	}
	return strings.Join(msgs, "; ")
}

// checkNacosDep TCP 拨号每个 nacos host:port，任一可达即视为可用(不启动 SDK,避免注册副作用)。
func checkNacosDep(cfg *config.CKManConfig) error {
	if len(cfg.Nacos.Hosts) == 0 {
		return nil
	}
	port := strconv.FormatUint(cfg.Nacos.Port, 10)
	var lastErr error
	for _, host := range cfg.Nacos.Hosts {
		addr := net.JoinHostPort(strings.TrimSpace(host), port)
		if err := dialTCP(addr); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func dialTCP(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}
```

> `net`/`strconv` 已真实使用，删除 Task 1 对应 `var _` 占位。

- [ ] **Step 4: 运行测试**

Run: `go test ./cmd/watchdog/ -run TestDBAddr -v`
Expected: PASS（4 个）。

- [ ] **Step 5: 提交**

```bash
git add cmd/watchdog/watchdog.go cmd/watchdog/watchdog_test.go
git commit -m "feat(watchdog): 依赖探测(local 跳过;mysql/pg/dm8 与 nacos 仅 TCP 拨号)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: verdict 状态机 probe()

**Files:**
- Modify: `cmd/watchdog/watchdog.go`

- [ ] **Step 1: 实现 probe()**

在 `cmd/watchdog/watchdog.go` 追加：

```go
// ---------------- 探测主流程 ----------------

// probe 执行一轮探测。deep=true 时附加 DB/Nacos 依赖探测(仅自愈模式用)。
func probe(cfg *config.CKManConfig, p *paths, deep bool) probeResult {
	pids := findCkmanPids(*p)

	if len(pids) == 0 {
		// 真相源：进程不存在 → 查 pidfile 解释原因
		if fileExists(p.pidfile) {
			return probeResult{v: vCrash, evidence: "进程不存在 + pidfile 残留(" + p.pidfile + ")"}
		}
		return probeResult{v: vStopped, evidence: "进程不存在 + 无 pidfile(运维主动停/优雅退出)"}
	}
	if len(pids) > 1 {
		return probeResult{v: vMulti, pid: pids[0], evidence: fmt.Sprintf("扫描到多个 ckman 进程: %v", pids)}
	}
	pid := pids[0]

	// 进程状态层
	switch procState(pid) {
	case "T":
		return probeResult{v: vHung, pid: pid, evidence: "进程状态 T(已停止)"}
	case "D":
		if persistentD(pid) {
			return probeResult{v: vHung, pid: pid, evidence: fmt.Sprintf("进程持续 D 态(采样 %d 次)", dStateSamples)}
		}
	}

	// 接口层：HTTP 超时/拒绝 → 局部重试 → 仍无应答判夯死
	code, ok := httpProbeWithRetry(cfg)
	if !ok {
		return probeResult{v: vHung, pid: pid, evidence: "进程存活但接口无应答(超时/拒绝，已重试)"}
	}
	if code != 200 {
		return probeResult{v: vApp, pid: pid, httpCode: code, evidence: fmt.Sprintf("接口返回 HTTP %d", code)}
	}

	// 依赖层(仅自愈模式做，仅告警)
	if deep {
		if msg := depCheck(cfg); msg != "" {
			return probeResult{v: vDep, pid: pid, httpCode: 200, evidence: msg}
		}
	}
	suffix := ""
	if deep {
		suffix = " 且依赖正常"
	}
	return probeResult{v: vHealthy, pid: pid, httpCode: 200, evidence: "接口 200" + suffix}
}
```

- [ ] **Step 2: 编译验证**

Run: `cd /data/root/go/src/github.com/housepower/ckman && go build ./cmd/watchdog/`
Expected: 编译通过。

- [ ] **Step 3: 提交**

```bash
git add cmd/watchdog/watchdog.go
git commit -m "feat(watchdog): verdict 状态机 probe()(进程/状态/接口/依赖四层)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: 日志 / 告警（initLogger / openAlertWriter / writeAlert）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`（替换 Task 1 的 `initLogger` 空桩）

- [ ] **Step 1: 实现日志与告警**

先**删除** Task 1 末尾的 `func initLogger(...) {}` 空桩，然后追加：

```go
// ---------------- 日志/告警 ----------------

// initLogger 把 ckman 的全局 log.Logger 指向 watchdog.log(lumberjack 轮转,复用 cfg.Log)。
func initLogger(cfg *config.CKManConfig, p *paths) {
	_ = os.MkdirAll(filepath.Dir(p.watchdogLog), 0755)
	log.InitLogger(p.watchdogLog, &cfg.Log)
}

func openAlertWriter(cfg *config.CKManConfig, p *paths) {
	_ = os.MkdirAll(filepath.Dir(p.alertLog), 0755)
	alertWriter = &lumberjack.Logger{
		Filename:   p.alertLog,
		MaxSize:    cfg.Log.MaxSize,
		MaxBackups: cfg.Log.MaxCount,
		MaxAge:     cfg.Log.MaxAge,
		LocalTime:  true,
	}
}

// writeAlert 写 [HEAL]/[ALERT]/[CRIT] 到 watchdog.log，并追加到 ckman-alert.log。
// 特例 OK(健康心跳)：watchdog.log 已有 verdict 行，故只补 alert 文件，不重复刷。
func writeAlert(level, msg string) {
	switch level {
	case "CRIT":
		log.Logger.Errorf("[%s] %s", level, msg)
	case "OK":
		// 健康心跳:不重复打 watchdog.log，仅落 alert 文件
	default:
		log.Logger.Warnf("[%s] %s", level, msg)
	}
	if alertWriter != nil {
		line := fmt.Sprintf("%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), level, msg)
		_, _ = alertWriter.Write([]byte(line))
	}
}
```

> `lumberjack`/`log` 已真实使用，删除 Task 1 对应 `var _` 占位（若有）。

- [ ] **Step 2: 编译验证**

Run: `go build ./cmd/watchdog/`
Expected: 编译通过（`initLogger` 重复定义？确认 Task 1 空桩已删）。

- [ ] **Step 3: 提交**

```bash
git add cmd/watchdog/watchdog.go
git commit -m "feat(watchdog): 日志(watchdog.log)与告警(ckman-alert.log)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: 自愈主流程（runHeal / healRestart / runStart / waitGone / flock）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`（替换 Task 1 的 `runHeal` 空桩）

- [ ] **Step 1: 实现自愈 + flock**

先**删除** Task 1 末尾的 `func runHeal(...) {}` 空桩，然后追加：

```go
// ---------------- 自愈模式 ----------------

func runHeal(cfg *config.CKManConfig, p *paths) {
	// 单例锁：防 cron 叠跑
	lock, ok := tryFlock(p.lockFile)
	if !ok {
		log.Logger.Infof("[watchdog] 另一实例持锁，本轮跳过")
		return
	}
	defer releaseFlock(lock)

	openAlertWriter(cfg, p)

	restarts := readRestarts(p.restartsFile)

	// 启动宽限：距上次拉起过近则跳过(避免刚起又判)
	if last := latest(restarts); !last.IsZero() && time.Since(last) < startupGrace {
		log.Logger.Infof("[watchdog] 启动宽限期内(距上次拉起 %.0fs < %.0fs)，本轮跳过",
			time.Since(last).Seconds(), startupGrace.Seconds())
		return
	}

	res := probe(cfg, p, true)
	log.Logger.Infof("[watchdog] verdict=%s pid=%d %s", res.v, res.pid, res.evidence)

	switch res.v {
	case vHealthy:
		writeAlert("OK", "ckman 健康: "+res.evidence)
	case vStopped:
		// 无动作(运维主动停)
	case vMulti, vApp, vDep:
		writeAlert("ALERT", fmt.Sprintf("%s: %s", res.v, res.evidence))
	case vCrash, vHung:
		healRestart(cfg, p, res, restarts)
	}
}

func healRestart(cfg *config.CKManConfig, p *paths, res probeResult, restarts []time.Time) {
	recent := countWithin(restarts, restartWindow)

	// 重启风暴保护
	if recent >= restartMax {
		writeAlert("CRIT", fmt.Sprintf("放弃自动重启：窗口 %s 内已重启 %d 次(≥%d)，需人工介入。触发=%s(%s)",
			restartWindow, recent, restartMax, res.v, res.evidence))
		return
	}

	// 夯死先 kill -9
	if res.v == vHung && res.pid > 0 {
		if killErr := syscall.Kill(res.pid, syscall.SIGKILL); killErr != nil {
			writeAlert("HEAL", fmt.Sprintf("kill -9 pid=%d 失败: %v (触发=%s)", res.pid, killErr, res.evidence))
		} else {
			writeAlert("HEAL", fmt.Sprintf("kill -9 pid=%d 成功 (触发=%s)", res.pid, res.evidence))
		}
		waitGone(res.pid, 5*time.Second)
	}

	// 拉起
	out, startErr := runStart(p)

	// 记录重启时间戳并按 max(window,grace) 裁剪
	restarts = append(restarts, time.Now())
	keep := restartWindow
	if startupGrace > keep {
		keep = startupGrace
	}
	writeRestarts(p.restartsFile, pruneWithin(restarts, keep))

	if startErr != nil {
		writeAlert("CRIT", fmt.Sprintf("拉起失败: %v | 触发=%s(%s) | start=%q | 输出=%s",
			startErr, res.v, res.evidence, p.startCmd, strings.TrimSpace(out)))
		return
	}
	writeAlert("HEAL", fmt.Sprintf("已拉起 ckman | 触发=%s(%s) | 窗口 %s 内第 %d/%d 次 | start=%q",
		res.v, res.evidence, restartWindow, recent+1, restartMax, p.startCmd))
}

// runStart 执行拉起命令(默认 <HDIR>/bin/start，内部已带 -d 自守护)。
func runStart(p *paths) (string, error) {
	cmd := exec.Command(p.startCmd)
	cmd.Dir = p.hdir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func waitGone(pid int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if syscall.Kill(pid, 0) != nil { // 进程已不存在
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---------------- flock ----------------

func tryFlock(path string) (*os.File, bool) {
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, false
	}
	return f, true
}

func releaseFlock(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}
```

> `syscall`/`exec` 已真实使用，删除 Task 1 对应 `var _` 占位。

- [ ] **Step 2: 编译验证**

Run: `go build ./cmd/watchdog/`
Expected: 编译通过。

- [ ] **Step 3: 提交**

```bash
git add cmd/watchdog/watchdog.go
git commit -m "feat(watchdog): 自愈主流程(flock 单例/启动宽限/kill+拉起/重启风暴熔断)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: -check 模式 + 退出码映射（TDD）

**Files:**
- Modify: `cmd/watchdog/watchdog.go`（替换 Task 1 的 `runCheck` 空桩）
- Test: `cmd/watchdog/watchdog_test.go`

- [ ] **Step 1: 写 checkExitCode 失败测试**

在 `watchdog_test.go` 追加：

```go
func TestCheckExitCode(t *testing.T) {
	cases := []struct {
		res  probeResult
		code int
		lbl  string
	}{
		{probeResult{v: vHealthy}, 0, "OK"},
		{probeResult{v: vCrash}, 10, "DOWN"},
		{probeResult{v: vStopped}, 10, "DOWN"},
		{probeResult{v: vHung}, 11, "HUNG"},
		{probeResult{v: vApp, httpCode: 500}, 12, "APP"},
		{probeResult{v: vApp, httpCode: 401}, 3, "AUTH"},
		{probeResult{v: vMulti}, 12, "ABNORMAL"},
	}
	for _, c := range cases {
		code, lbl := checkExitCode(c.res)
		if code != c.code || lbl != c.lbl {
			t.Errorf("verdict=%s code=%d: want (%d,%s) got (%d,%s)",
				c.res.v, c.res.httpCode, c.code, c.lbl, code, lbl)
		}
	}
}
```

- [ ] **Step 2: 运行，确认失败**

Run: `go test ./cmd/watchdog/ -run TestCheckExitCode -v`
Expected: `undefined: checkExitCode`。

- [ ] **Step 3: 实现 runCheck + checkExitCode**

先**删除** Task 1 末尾的 `func runCheck(...) {}` 空桩，然后追加：

```go
// ---------------- -check 模式 ----------------

func runCheck(cfg *config.CKManConfig, p *paths) {
	// 即便健康也留痕:仅打 stdout 时健康=无输出,无法区分"未执行"与"静默成功"。
	initLogger(cfg, p)
	res := probe(cfg, p, false) // 不做依赖深探，轻量
	code, label := checkExitCode(res)
	line := fmt.Sprintf("%s pid=%d %s", label, res.pid, res.evidence)
	fmt.Println(line)
	log.Logger.Infof("[watchdog] -check %s", line)
	os.Exit(code)
}

// checkExitCode 把 verdict 映射为退出码：0 健康 · 10 DOWN · 11 HUNG · 12 APP/异常 · 3 鉴权。
func checkExitCode(res probeResult) (int, string) {
	switch res.v {
	case vHealthy:
		return 0, "OK"
	case vCrash, vStopped:
		return 10, "DOWN"
	case vHung:
		return 11, "HUNG"
	case vApp:
		if res.httpCode == 401 {
			return 3, "AUTH"
		}
		return 12, "APP"
	case vMulti:
		return 12, "ABNORMAL"
	}
	return 1, "UNKNOWN"
}
```

- [ ] **Step 4: 运行测试**

Run: `go test ./cmd/watchdog/ -run TestCheckExitCode -v`
Expected: PASS。

- [ ] **Step 5: 全量测试 + vet**

Run: `cd /data/root/go/src/github.com/housepower/ckman && go test ./cmd/watchdog/ -v && go vet ./cmd/watchdog/`
Expected: 所有测试 PASS，vet 无输出。

- [ ] **Step 6: 提交**

```bash
git add cmd/watchdog/watchdog.go cmd/watchdog/watchdog_test.go
git commit -m "feat(watchdog): -check 探测模式与退出码映射(TDD)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: Makefile 构建/打包 + 文档

**Files:**
- Modify: `Makefile`
- Create: `docs/watchdog.md`

- [ ] **Step 1: Makefile backend 目标增加 watchdog 构建**

在 `Makefile` 的 `backend:` 目标里，`ckmanctl` 那行之后增加一行（保持与现有缩进/写法一致）：

原（约 69-72 行）:
```makefile
backend:
	...
	CGO_ENABLED=0 go build ${LDFLAGS}
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/ckmanctl/ckmanctl cmd/ckmanctl/ckmanctl.go
```
改为追加：
```makefile
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/watchdog/watchdog cmd/watchdog/watchdog.go
```

`debug:` 目标同样在 ckmanctl 行后追加：
```makefile
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/watchdog/watchdog cmd/watchdog/watchdog.go
```

- [ ] **Step 2: Makefile package 目标拷贝 watchdog 到 bin**

在 `package:` 目标里，`ckmanctl` 拷贝行（约 113 行 `@mv ${SHDIR}/cmd/ckmanctl/ckmanctl ${PKGFULLDIR_TMP}/bin`）之后追加：
```makefile
	@mv ${SHDIR}/cmd/watchdog/watchdog ${PKGFULLDIR_TMP}/bin
```

- [ ] **Step 3: 验证 backend 构建**

Run: `cd /data/root/go/src/github.com/housepower/ckman && make backend 2>&1 | tail -20 && ls -l cmd/watchdog/watchdog`
Expected: 构建成功，`cmd/watchdog/watchdog` 存在。

- [ ] **Step 4: 写部署文档**

创建 `docs/watchdog.md`：

````markdown
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

- 探活端点：`/metrics`(无鉴权，默认开启)；关闭 metric 时回退 `/`。
- 依赖探测零副作用：`local` 策略不探 DB；`mysql/postgres/dm8` 与 Nacos 仅 TCP 拨号；不读密码、不连库迁移。
- 防重启风暴：10 分钟内最多重启 3 次，超限熔断只告警；距上次拉起 60s 内跳过。

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
````

- [ ] **Step 5: 提交**

```bash
git add Makefile docs/watchdog.md
git commit -m "build+docs(watchdog): Makefile 构建/打包接入 + 部署文档

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 11: 端到端冒烟验证

**Files:** 无（仅验证）

- [ ] **Step 1: 全量测试 + vet + 构建**

Run:
```bash
cd /data/root/go/src/github.com/housepower/ckman
go test ./cmd/watchdog/ -v
go vet ./cmd/watchdog/
go build -o cmd/watchdog/watchdog ./cmd/watchdog/
```
Expected: 测试全 PASS，vet 无输出，构建成功，产物 `cmd/watchdog/watchdog` 就位（供后续步骤调用）。

- [ ] **Step 2: `-check` 对真实/缺失配置的行为**

Run（无 ckman 运行、用仓库模板配置）:
```bash
cd /data/root/go/src/github.com/housepower/ckman
./cmd/watchdog/watchdog -c resources/ckman.hjson -pid /tmp/nonexist.pid -check; echo "exit=$?"
```
Expected: 打印 `DOWN pid=0 进程不存在 + 无 pidfile(...)`，`exit=10`（进程不在且无 pidfile → STOPPED → 退出码 10）。

- [ ] **Step 3: 模拟 CRASH 判定（pidfile 残留）**

Run:
```bash
cd /data/root/go/src/github.com/housepower/ckman
touch /tmp/fake-ckman.pid
./cmd/watchdog/watchdog -c resources/ckman.hjson -pid /tmp/fake-ckman.pid -check; echo "exit=$?"
rm -f /tmp/fake-ckman.pid
```
Expected: 打印含 `进程不存在 + pidfile 残留`，`exit=10`（CRASH → DOWN）。

> 说明：`-check` 不自愈，故不会真的拉起。CRASH 与 STOPPED 在 `-check` 下同为退出码 10(DOWN)，但 evidence 文案不同，可据此区分。自愈行为(真正拉起/kill)需在装有完整 tarball 的环境用 crontab 或手动 `bin/watchdog`(不带 -check)验证，不在本地仓库冒烟范围内。

- [ ] **Step 4: 确认 watchdog.log 留痕并清理开发态产物**

Run:
```bash
cd /data/root/go/src/github.com/housepower/ckman
ls -l cmd/logs/watchdog.log 2>/dev/null && echo "--- 留痕内容 ---" && cat cmd/logs/watchdog.log
rm -rf cmd/logs cmd/watchdog/watchdog   # 清理开发态二进制位置推导出的 HDIR=cmd 产生的日志与本地二进制
```
Expected: 看到 `-check` 在 `cmd/logs/watchdog.log` 写入了带时间戳的痕迹行（开发态二进制在 `cmd/watchdog/watchdog`，HDIR 推导为 `cmd/`，故日志落 `cmd/logs/`；生产态二进制在 `<HDIR>/bin/`，日志落 `<HDIR>/logs/`）。清理后仓库无残留。

- [ ] **Step 5: 最终确认（无遗留占位）**

Run: `grep -n 'var _ =' cmd/watchdog/watchdog.go || echo "无遗留占位,OK"`
Expected: `无遗留占位,OK`（确认 Task 1 的临时 `var _` 占位均已在后续任务删除）。

---

## Self-Review 备注

- **Spec 覆盖**：需求1(CRASH→拉起 T8)、需求2(HUNG→kill+拉起 T8)、需求3(探 nacos/db/http T4·T5)、需求4(STOPPED 不干预 T6·T8)、需求5(DEP 仅告警+防风暴 T5·T8)、零配置(无配置段 T1)、local 不探(T5)、版本无关(T1·T5)、`-check`(T9)、构建打包(T10)，均有对应任务。
- **类型一致性**：`verdict`/`probeResult`/`paths` 字段与方法名全计划统一；`findCkmanPids(paths)` 取值传参，`probe`/`runHeal` 用 `*paths`。
- **占位清理**：Task 1 的临时 `var _ =` 与三个空桩函数，分别在 T2/T3/T4/T5/T7/T8/T9 用真实定义替换/删除；T11 Step5 做最终校验。
