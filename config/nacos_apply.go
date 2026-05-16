package config

import (
	"sync"
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
