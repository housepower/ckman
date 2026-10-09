package discovery

import (
	"reflect"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/service/discovery/nacos"
	"github.com/pkg/errors"
)

// NewRegistry 按 cfg.Registry.Type 创建注册中心。cacheDir 供 SDK 存放缓存与日志。
func NewRegistry(cfg *config.CKManConfig, cacheDir string) (Registry, error) {
	switch cfg.Registry.Type {
	case "", config.DISCOVERY_NONE:
		return &noopRegistry{}, nil
	case config.DISCOVERY_NACOS:
		return nacos.NewRegistry(&cfg.Nacos, cfg.Registry.ServiceName, cacheDir)
	}
	return nil, errors.Errorf("unsupported registry type %q", cfg.Registry.Type)
}

// NewConfigSource 按 cfg.ConfigCenter.Type 创建配置中心。
func NewConfigSource(cfg *config.CKManConfig, cacheDir string) (ConfigSource, error) {
	switch cfg.ConfigCenter.Type {
	case "", config.DISCOVERY_NONE:
		return noopSource{}, nil
	case config.DISCOVERY_NACOS:
		return nacos.NewConfigSource(&cfg.Nacos, cfg.ConfigFile, cacheDir)
	}
	return nil, errors.Errorf("unsupported config_center type %q", cfg.ConfigCenter.Type)
}

// LoadInitial 在启动阶段从配置中心拉取配置并合并到 cfg。任何错误（不可达、内容为空、
// 解析失败）都只记录日志并沿用本地配置，启动绝不阻塞。返回 cfg.Log 是否被改变，
// 调用方据此重建 logger（ApplyInitialRemote 故意不触发 applier）。
func LoadInitial(src ConfigSource, cfg *config.CKManConfig) (logChanged bool) {
	if !cfg.ConfigCenter.Enabled() {
		return false
	}
	data, format, err := src.Load()
	if err != nil {
		log.Logger.Warnf("%s: load remote config failed, fallback to local: %v", cfg.ConfigCenter.Type, err)
		return false
	}
	if len(data) == 0 {
		log.Logger.Warnf("%s: remote config is empty, fallback to local", cfg.ConfigCenter.Type)
		return false
	}
	logBefore := cfg.Log
	if err := config.ApplyInitialRemote(data, format, cfg); err != nil {
		log.Logger.Errorf("%s: apply initial remote config failed: %v", cfg.ConfigCenter.Type, err)
		return false
	}
	return !reflect.DeepEqual(logBefore, cfg.Log)
}

// Watch 监听配置中心变更并热生效。必须在所有 applier 注册之后调用，
// 否则早到的推送会漏掉尚未注册的 applier。
func Watch(src ConfigSource) error {
	return src.Watch(func(data []byte, format string) {
		if err := config.ApplyRemoteUpdate(data, format); err != nil {
			log.Logger.Errorf("apply remote config failed: %v", err)
		}
	})
}
