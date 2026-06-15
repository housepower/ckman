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
