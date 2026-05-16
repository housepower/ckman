package config

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/hjson/hjson-go/v4"
	"gopkg.in/yaml.v3"
)

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
