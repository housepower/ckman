// Package consul 实现基于 Consul 的注册中心（TTL 健康检查 + blocking query）与 KV 配置中心。
// 设计见 docs/superpowers/specs/2026-10-09-registry-config-split-design.md。
package consul

import (
	"context"
	"fmt"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/housepower/ckman/config"
)

type timing struct {
	checkInterval   time.Duration
	checkTTL        time.Duration
	deregisterAfter time.Duration
	waitTime        time.Duration
}

func parseTiming(c *config.CKManConsulConfig) (timing, error) {
	var t timing
	for _, d := range []struct {
		name string
		val  string
		out  *time.Duration
	}{
		{"check_interval", c.CheckInterval, &t.checkInterval},
		{"check_ttl", c.CheckTTL, &t.checkTTL},
		{"deregister_after", c.DeregisterAfter, &t.deregisterAfter},
		{"wait_time", c.WaitTime, &t.waitTime},
	} {
		v, err := time.ParseDuration(d.val)
		if err != nil || v <= 0 {
			return t, fmt.Errorf("invalid consul.%s %q", d.name, d.val)
		}
		*d.out = v
	}
	// 心跳间隔不小于 TTL 时，实例会周期性地变成 critical。
	if t.checkInterval >= t.checkTTL {
		return t, fmt.Errorf("consul.check_interval(%s) must be < consul.check_ttl(%s)", c.CheckInterval, c.CheckTTL)
	}
	return t, nil
}

func newClient(c *config.CKManConsulConfig) (*consulapi.Client, error) {
	cfg := consulapi.DefaultConfig()
	cfg.Address = c.Address
	cfg.Scheme = c.Scheme
	cfg.Datacenter = c.Datacenter
	cfg.Token = c.Token
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create consul client: %w", err)
	}
	return client, nil
}

// sleepCtx 可取消地睡眠 d，ctx 取消时返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextBackoff 指数退避，上限 30s。
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

const initialBackoff = 500 * time.Millisecond

// nextIndex 按 Consul blocking query 的约定处理索引：索引回退（如 server 重建）时
// 必须从 0 重新开始，否则会一直挂在一个永远不会到达的索引上。
func nextIndex(last, got uint64) uint64 {
	if got < last {
		return 0
	}
	return got
}
