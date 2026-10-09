package discovery

import (
	"github.com/housepower/ckman/config"
)

// noopRegistry 用于未启用注册中心的单机部署：成员列表只有自己。
type noopRegistry struct {
	onChange func([]config.ClusterNode)
}

func (r *noopRegistry) Start(self config.ClusterNode) error {
	if r.onChange != nil {
		r.onChange([]config.ClusterNode{self})
	}
	return nil
}

func (r *noopRegistry) Stop() error { return nil }

func (r *noopRegistry) OnChange(fn func([]config.ClusterNode)) { r.onChange = fn }

type noopSource struct{}

func (noopSource) Load() ([]byte, string, error)    { return nil, "", nil }
func (noopSource) Watch(func([]byte, string)) error { return nil }
func (noopSource) Close() error                     { return nil }
