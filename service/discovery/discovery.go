// Package discovery 抽象注册中心与配置中心，两者互相独立，后端可以任意组合。
// 设计见 docs/superpowers/specs/2026-10-09-registry-config-split-design.md。
package discovery

import (
	"github.com/housepower/ckman/config"
)

// Registry 负责注册本实例并维护集群成员列表。
type Registry interface {
	// Start 订阅成员变更并注册本实例；成员变更通过 OnChange 设置的回调通知。
	Start(self config.ClusterNode) error
	// Stop 反注册并释放资源。best-effort：任一步失败也会继续执行后续步骤。
	Stop() error
	// OnChange 设置成员变更回调，必须在 Start 之前调用；回调收到的是完整成员快照。
	OnChange(func([]config.ClusterNode))
}

// ConfigSource 负责拉取并监听远端配置。
type ConfigSource interface {
	// Load 拉取一次完整配置，format 为 config.FORMAT_*。远端没有配置时返回空内容、nil 错误。
	Load() (data []byte, format string, err error)
	// Watch 开始监听变更，回调收到的是完整内容。
	Watch(func(data []byte, format string)) error
	Close() error
}
