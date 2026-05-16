package config

import (
	"fmt"
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
