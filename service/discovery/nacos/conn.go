// Package nacos 实现基于 Nacos 的注册中心与配置中心。两者共用 nacos 配置段里的连接参数，
// 各自按需创建 naming client / config client。
package nacos

import (
	"github.com/housepower/ckman/config"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
)

func clientConfig(c *config.CKManNacosConfig, namespaceId, cacheDir string) *constant.ClientConfig {
	return &constant.ClientConfig{
		BeatInterval:        c.BeatInterval,
		NamespaceId:         namespaceId,
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              cacheDir,
		CacheDir:            cacheDir,
		LogLevel:            "info",
		Username:            c.UserName,
		Password:            c.Password,
	}
}

func serverConfigs(c *config.CKManNacosConfig) []constant.ServerConfig {
	var servers []constant.ServerConfig
	for _, host := range c.Hosts {
		servers = append(servers, constant.ServerConfig{
			IpAddr:      host,
			ContextPath: "/nacos",
			Port:        c.Port,
		})
	}
	return servers
}
