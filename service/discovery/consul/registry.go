package consul

import (
	"context"
	"fmt"
	"sync"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
)

type Registry struct {
	client   *consulapi.Client
	service  string
	timing   timing
	onChange func([]config.ClusterNode)

	reg    *consulapi.AgentServiceRegistration
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewRegistry(c *config.CKManConsulConfig, service string) (*Registry, error) {
	t, err := parseTiming(c)
	if err != nil {
		return nil, err
	}
	client, err := newClient(c)
	if err != nil {
		return nil, err
	}
	return &Registry{client: client, service: service, timing: t}, nil
}

func (r *Registry) OnChange(fn func([]config.ClusterNode)) { r.onChange = fn }

func (r *Registry) Start(self config.ClusterNode) error {
	id := fmt.Sprintf("%s-%s-%d", r.service, self.Ip, self.Port)
	r.reg = &consulapi.AgentServiceRegistration{
		ID:      id,
		Name:    r.service,
		Address: self.Ip,
		Port:    self.Port,
		Check: &consulapi.AgentServiceCheck{
			CheckID:                        "ttl:" + id,
			TTL:                            r.timing.checkTTL.String(),
			Status:                         consulapi.HealthPassing,
			DeregisterCriticalServiceAfter: r.timing.deregisterAfter.String(),
		},
	}
	if err := r.client.Agent().ServiceRegister(r.reg); err != nil {
		return fmt.Errorf("consul service register: %w", err)
	}
	log.Logger.Infof("consul registered service %s id %s", r.service, id)

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(2)
	go r.keepAlive()
	go r.watch()
	return nil
}

func (r *Registry) Stop() error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	r.wg.Wait()
	if err := r.client.Agent().ServiceDeregister(r.reg.ID); err != nil {
		return fmt.Errorf("consul service deregister: %w", err)
	}
	return nil
}

// keepAlive 周期性上报 TTL pass。上报失败时（如 agent 重启后丢了注册）重新注册。
func (r *Registry) keepAlive() {
	defer r.wg.Done()
	checkID := r.reg.Check.CheckID
	for {
		err := r.client.Agent().UpdateTTLOpts(checkID, "", consulapi.HealthPassing,
			(&consulapi.QueryOptions{}).WithContext(r.ctx))
		if err != nil && r.ctx.Err() == nil {
			log.Logger.Warnf("consul ttl pass %s failed, re-register: %v", checkID, err)
			if err := r.client.Agent().ServiceRegister(r.reg); err != nil {
				log.Logger.Warnf("consul re-register %s failed: %v", r.reg.ID, err)
			}
		}
		if !sleepCtx(r.ctx, r.timing.checkInterval) {
			return
		}
	}
}

// watch 用 blocking query 订阅健康实例列表，索引变化时回调完整快照。
func (r *Registry) watch() {
	defer r.wg.Done()
	var lastIndex uint64
	backoff := initialBackoff
	for {
		q := (&consulapi.QueryOptions{WaitIndex: lastIndex, WaitTime: r.timing.waitTime}).WithContext(r.ctx)
		entries, meta, err := r.client.Health().Service(r.service, "", true, q)
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			log.Logger.Warnf("consul watch service %s failed: %v", r.service, err)
			if !sleepCtx(r.ctx, backoff) {
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
			continue // 等待超时，无变化
		}
		lastIndex = nextIndex(lastIndex, meta.LastIndex)

		nodes := make([]config.ClusterNode, 0, len(entries))
		for _, e := range entries {
			if e.Service == nil {
				continue
			}
			addr := e.Service.Address
			if addr == "" { // 注册时未填 Address 则取节点地址
				addr = e.Node.Address
			}
			nodes = append(nodes, config.ClusterNode{Ip: addr, Port: e.Service.Port})
		}
		log.Logger.Infof("consul instances %v", nodes)
		if r.onChange != nil {
			r.onChange(nodes)
		}
	}
}
