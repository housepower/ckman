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
var _ = strconv.Itoa      // 占位,Task 2 删
var _ = strings.TrimSpace // 占位,Task 2 删
var _ = syscall.Kill      // 占位,Task 8 删
var _ = procfs.AllProcs   // 占位,Task 3 删
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
