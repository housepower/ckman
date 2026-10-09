package nacos

import (
	"fmt"
	"path/filepath"

	"github.com/housepower/ckman/config"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type ConfigSource struct {
	client config_client.IConfigClient
	dataId string
	group  string
	format string
}

// NewConfigSource 创建 Nacos 配置中心。内容格式跟随本地配置文件后缀。
func NewConfigSource(c *config.CKManNacosConfig, configFile, cacheDir string) (*ConfigSource, error) {
	// 配置中心可以使用与注册中心不同的 namespace（借鉴 cell）。
	namespaceId := c.NamespaceId
	if c.CfgNamespaceId != "" {
		namespaceId = c.CfgNamespaceId
		if namespaceId == "public" {
			namespaceId = ""
		}
	}
	client, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  clientConfig(c, namespaceId, cacheDir),
		ServerConfigs: serverConfigs(c),
	})
	if err != nil {
		return nil, fmt.Errorf("create nacos config client: %w", err)
	}
	return &ConfigSource{
		client: client,
		dataId: c.DataID,
		group:  c.Group,
		format: filepath.Ext(configFile),
	}, nil
}

func (s *ConfigSource) Load() ([]byte, string, error) {
	content, err := s.client.GetConfig(vo.ConfigParam{DataId: s.dataId, Group: s.group})
	if err != nil && err.Error() != "config not found" {
		return nil, "", err
	}
	return []byte(content), s.format, nil
}

func (s *ConfigSource) Watch(fn func([]byte, string)) error {
	return s.client.ListenConfig(vo.ConfigParam{
		DataId: s.dataId,
		Group:  s.group,
		OnChange: func(_, _, _, data string) {
			fn([]byte(data), s.format)
		},
	})
}

func (s *ConfigSource) Close() error {
	err := s.client.CancelListenConfig(vo.ConfigParam{DataId: s.dataId, Group: s.group})
	s.client.CloseClient()
	return err
}
