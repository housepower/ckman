package consul

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
)

// ConfigSource 用 Consul KV 的一个 key 存放整份配置。Consul KV 不保留历史，
// 回滚需要依赖外部工具（如 git2consul）。
type ConfigSource struct {
	client *consulapi.Client
	key    string
	format string
	timing timing

	mu        sync.Mutex
	lastIndex uint64
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func NewConfigSource(c *config.CKManConsulConfig) (*ConfigSource, error) {
	t, err := parseTiming(c)
	if err != nil {
		return nil, err
	}
	format := filepath.Ext(c.KVKey)
	switch format {
	case config.FORMAT_HJSON, config.FORMAT_JSON, config.FORMAT_YAML:
	default:
		return nil, fmt.Errorf("consul.kv_key %q must end with .hjson, .json or .yaml", c.KVKey)
	}
	client, err := newClient(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ConfigSource{client: client, key: c.KVKey, format: format, timing: t, ctx: ctx, cancel: cancel}, nil
}

func (s *ConfigSource) Load() ([]byte, string, error) {
	pair, meta, err := s.client.KV().Get(s.key, (&consulapi.QueryOptions{}).WithContext(s.ctx))
	if err != nil {
		return nil, "", fmt.Errorf("consul kv get %s: %w", s.key, err)
	}
	s.mu.Lock()
	s.lastIndex = meta.LastIndex
	s.mu.Unlock()
	if pair == nil {
		return nil, s.format, nil
	}
	return pair.Value, s.format, nil
}

func (s *ConfigSource) Watch(fn func([]byte, string)) error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.mu.Lock()
		lastIndex := s.lastIndex
		s.mu.Unlock()
		backoff := initialBackoff
		for {
			q := (&consulapi.QueryOptions{WaitIndex: lastIndex, WaitTime: s.timing.waitTime}).WithContext(s.ctx)
			pair, meta, err := s.client.KV().Get(s.key, q)
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				log.Logger.Warnf("consul watch kv %s failed: %v", s.key, err)
				if !sleepCtx(s.ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff)
				// 失败可能是 Consul 重启，索引已回退；带旧索引的请求会一直挂到 wait_time，
				// 从 0 重新开始可立即拿到当前快照。
				lastIndex = 0
				continue
			}
			backoff = initialBackoff
			if meta.LastIndex == lastIndex {
				continue
			}
			lastIndex = nextIndex(lastIndex, meta.LastIndex)
			if pair == nil {
				log.Logger.Warnf("consul kv %s deleted, keep current config", s.key)
				continue
			}
			fn(pair.Value, s.format)
		}
	}()
	return nil
}

func (s *ConfigSource) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}
