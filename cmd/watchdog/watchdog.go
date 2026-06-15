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

var _ = tls.Config{}      // 占位,Task 4 删
var _ = net.Dial          // 占位,Task 5 删
var _ = http.MethodGet    // 占位,Task 4 删
var _ = exec.Command      // 占位,Task 8 删
var _ = syscall.Kill      // 占位,Task 8 删
var _ = log.Logger        // 占位,Task 7 删

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

func runCheck(cfg *config.CKManConfig, p *paths) {}
func initLogger(cfg *config.CKManConfig, p *paths) {}
func runHeal(cfg *config.CKManConfig, p *paths) {}

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
