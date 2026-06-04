package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"

	"github.com/hjson/hjson-go/v4"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// nacosLogger is the logger used by the nacos_apply functions.
// It defaults to a no-op logger and can be set via SetNacosLogger.
var nacosLogger *zap.SugaredLogger

func init() {
	nacosLogger = zap.NewNop().Sugar()
}

// SetNacosLogger sets the logger used by ApplyNacosUpdate and ApplyInitialNacos.
// Call this once after the application logger is initialised (e.g. in main.go).
func SetNacosLogger(l *zap.SugaredLogger) {
	nacosLogger = l
}

// Applier 是热生效回调。old 为 merge 前的 GlobalConfig 快照，new 为 merge 后的 &GlobalConfig。
// Applier 必须自行 recover panic，必须 idempotent，不应假设并发调用。
type Applier func(old, new *CKManConfig)

type namedApplier struct {
	name string
	fn   Applier
}

var (
	appliersMu sync.Mutex
	appliers   []namedApplier

	// lastAppliedHash 保存上次成功 merge 的 sha256，用于去重 SDK 启动回放与重复推送。
	lastAppliedHash string
)

// RegisterApplier 注册热生效回调，按注册顺序串行执行。
func RegisterApplier(name string, fn Applier) {
	appliersMu.Lock()
	defer appliersMu.Unlock()
	appliers = append(appliers, namedApplier{name: name, fn: fn})
}

// ResetForTest 仅供测试使用，复位 GlobalConfig、applier 注册表与 lastAppliedHash。
func ResetForTest() {
	ConfigMutex.Lock()
	defer ConfigMutex.Unlock()
	appliersMu.Lock()
	defer appliersMu.Unlock()
	GlobalConfig = CKManConfig{}
	appliers = nil
	lastAppliedHash = ""
}

// mergeFromNacos 将 remote 中非 bootstrap、非保留段的非零字段覆盖到 local。
// 规则见 docs/superpowers/specs/2026-05-16-nacos-config-hotreload-design.md。
func mergeFromNacos(local, remote *CKManConfig) {
	// 1. Log：DeepEqual 零值判定后整段覆盖
	if !reflect.DeepEqual(remote.Log, CKManLogConfig{}) {
		local.Log = remote.Log
	}

	// 2. Cron：同上
	if !reflect.DeepEqual(remote.Cron, CronJob{}) {
		local.Cron = remote.Cron
	}

	// 3. ClickHouse：同上
	if !reflect.DeepEqual(remote.ClickHouse, ClickHouseOpts{}) {
		local.ClickHouse = remote.ClickHouse
	}

	// 4. Server 段内非 bootstrap 字段：单字段非零时覆盖（不能整段 DeepEqual）。
	if remote.Server.SessionTimeout != 0 {
		local.Server.SessionTimeout = remote.Server.SessionTimeout
	}
	if remote.Server.SwaggerEnable {
		local.Server.SwaggerEnable = remote.Server.SwaggerEnable
	}
	if remote.Server.PublicKey != "" {
		local.Server.PublicKey = remote.Server.PublicKey
	}
	if remote.Server.TaskInterval != 0 {
		local.Server.TaskInterval = remote.Server.TaskInterval
	}
	if remote.Server.BackupMaxConcurrent != 0 {
		local.Server.BackupMaxConcurrent = remote.Server.BackupMaxConcurrent
	}
	if remote.Server.Metric {
		local.Server.Metric = remote.Server.Metric
	}
	if remote.Server.MetricPath != "" {
		local.Server.MetricPath = remote.Server.MetricPath
	}
	if remote.Server.Pprof {
		local.Server.Pprof = remote.Server.Pprof
	}

	// ConfigFile / Version / Nacos / Server.Ip,Port,Https,Cert,Key,PkgPath / PersistentPolicy /
	// PersistentConfig 永不覆盖。
}

// bootstrapDiff 描述一个被忽略的 bootstrap 字段变更。
type bootstrapDiff struct {
	field string
	old   interface{}
	new   interface{}
}

// diffBootstrap 返回 remote 想改、但 merge 不会覆盖的 bootstrap 字段列表。
// 用于在 ApplyNacosUpdate 中输出 WARN 提示运维手动重启。
func diffBootstrap(local, remote *CKManConfig) []bootstrapDiff {
	var diffs []bootstrapDiff
	add := func(field string, oldV, newV interface{}) {
		diffs = append(diffs, bootstrapDiff{field: field, old: oldV, new: newV})
	}

	if local.Server.Ip != remote.Server.Ip && remote.Server.Ip != "" {
		add("Server.Ip", local.Server.Ip, remote.Server.Ip)
	}
	if local.Server.Port != remote.Server.Port && remote.Server.Port != 0 {
		add("Server.Port", local.Server.Port, remote.Server.Port)
	}
	if local.Server.Https != remote.Server.Https {
		add("Server.Https", local.Server.Https, remote.Server.Https)
	}
	if local.Server.CertFile != remote.Server.CertFile && remote.Server.CertFile != "" {
		add("Server.CertFile", local.Server.CertFile, remote.Server.CertFile)
	}
	if local.Server.KeyFile != remote.Server.KeyFile && remote.Server.KeyFile != "" {
		add("Server.KeyFile", local.Server.KeyFile, remote.Server.KeyFile)
	}
	if local.Server.PkgPath != remote.Server.PkgPath && remote.Server.PkgPath != "" {
		add("Server.PkgPath", local.Server.PkgPath, remote.Server.PkgPath)
	}
	if local.Server.PersistentPolicy != remote.Server.PersistentPolicy && remote.Server.PersistentPolicy != "" {
		add("Server.PersistentPolicy", local.Server.PersistentPolicy, remote.Server.PersistentPolicy)
	}
	if len(remote.PersistentConfig) > 0 && !reflect.DeepEqual(local.PersistentConfig, remote.PersistentConfig) {
		add("PersistentConfig", "<map>", "<map>")
	}
	if !reflect.DeepEqual(remote.Nacos, CKManNacosConfig{}) && !reflect.DeepEqual(local.Nacos, remote.Nacos) {
		add("Nacos", "<section>", "<section>")
	}
	return diffs
}

// parseRemote 按本地配置文件后缀解析 Nacos 推送的内容。
// fmtExt 应为 ".hjson" / ".json" / ".yaml"；其他后缀返回错误。
func parseRemote(data []byte, fmtExt string) (CKManConfig, error) {
	var out CKManConfig
	switch fmtExt {
	case FORMAT_HJSON, FORMAT_JSON:
		if err := hjson.Unmarshal(data, &out); err != nil {
			return CKManConfig{}, err
		}
	case FORMAT_YAML:
		if err := yaml.Unmarshal(data, &out); err != nil {
			return CKManConfig{}, err
		}
	default:
		return CKManConfig{}, fmt.Errorf("unsupported config format %q", fmtExt)
	}
	return out, nil
}

// hashContent 返回 data 的 sha256 十六进制串，用于内容去重。
func hashContent(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ApplyInitialNacos 在启动阶段使用：把 Nacos 返回的内容合并到 cfg，并设置 lastAppliedHash。
// 不触发 applier（启动期各服务尚未 Start）。
func ApplyInitialNacos(data []byte, fmtExt string, cfg *CKManConfig) error {
	remote, err := parseRemote(data, fmtExt)
	if err != nil {
		return fmt.Errorf("parse nacos config: %w", err)
	}
	ConfigMutex.Lock()
	defer ConfigMutex.Unlock()
	diffs := diffBootstrap(cfg, &remote)
	mergeFromNacos(cfg, &remote)
	lastAppliedHash = hashContent(data)
	for _, d := range diffs {
		nacosLogger.Warnf(
			"nacos config: bootstrap field %s differs from local (local=%v, remote=%v), ignored; local value will be used",
			d.field, d.old, d.new,
		)
	}
	nacosLogger.Infof("nacos config initial merge done, sha=%s", lastAppliedHash)
	return nil
}

// ApplyNacosUpdate 在 Nacos OnChange 回调中使用：解析、合并、触发 applier。
// 解析失败立即返回，不进入 merge；内容与上次相同则跳过。
func ApplyNacosUpdate(data []byte, fmtExt string) error {
	remote, err := parseRemote(data, fmtExt)
	if err != nil {
		return fmt.Errorf("parse nacos config: %w", err)
	}
	hash := hashContent(data)

	ConfigMutex.Lock()
	if hash == lastAppliedHash {
		ConfigMutex.Unlock()
		nacosLogger.Debugf("nacos config unchanged (sha=%s), skip", hash)
		return nil
	}
	// 值拷贝即可：CKManConfig 中唯一的引用类型 PersistentConfig 是 bootstrap 字段，
	// mergeFromNacos 不会改写它，因此 oldSnapshot 与 GlobalConfig 不会通过 map 别名互相影响。
	oldSnapshot := GlobalConfig
	diffs := diffBootstrap(&GlobalConfig, &remote)
	mergeFromNacos(&GlobalConfig, &remote)
	lastAppliedHash = hash
	newSnapshot := GlobalConfig
	appliersMu.Lock()
	current := make([]namedApplier, len(appliers))
	copy(current, appliers)
	appliersMu.Unlock()
	ConfigMutex.Unlock()

	nacosLogger.Infof("nacos config received, sha=%s", hash)
	for _, d := range diffs {
		nacosLogger.Warnf(
			"nacos config: bootstrap field %s changed (local=%v, remote=%v), "+
				"ignored in memory; restart ckman manually to apply",
			d.field, d.old, d.new,
		)
	}
	for _, a := range current {
		runApplier(a, &oldSnapshot, &newSnapshot)
	}
	nacosLogger.Infof("nacos config applied successfully")
	return nil
}

// runApplier 在独立的 deferred recover 中执行 a.fn。
func runApplier(a namedApplier, old, new *CKManConfig) {
	defer func() {
		if r := recover(); r != nil {
			nacosLogger.Errorf("applier %s panic: %v", a.name, r)
		}
	}()
	a.fn(old, new)
}
