package migrate

import (
	"fmt"
	"io"
	"os"

	"github.com/hjson/hjson-go/v4"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
	"github.com/pkg/errors"
)

type PersistentConfig struct {
	Policy string
	Config map[string]interface{}
}

type MigrateConfig struct {
	Source string
	Target string
	PsConf map[string]PersistentConfig `json:"persistent_config"`
}

func ParseConfig(conf string) (MigrateConfig, error) {
	var config MigrateConfig
	f, err := os.Open(conf)
	if err != nil {
		return MigrateConfig{}, errors.Wrap(err, "")
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return MigrateConfig{}, errors.Wrap(err, "")
	}
	if len(data) == 0 {
		return MigrateConfig{}, errors.New("empty config file")
	}
	err = hjson.Unmarshal(data, &config)
	if err != nil {
		return MigrateConfig{}, errors.Wrap(err, "")
	}
	return config, nil
}

func PersistentCheck(config MigrateConfig, typo string) (repository.PersistentMgr, error) {
	var ps repository.PersistentMgr
	conf, ok := config.PsConf[typo]
	if !ok {
		return nil, errors.Errorf("empty persistent config %s", config.Source)
	}
	ps = repository.GetPersistentByName(conf.Policy)
	if ps == nil {
		return nil, errors.Errorf("invalid persistent policy: %s", conf.Policy)
	}
	pcfg := ps.UnmarshalConfig(conf.Config)
	if err := ps.Init(pcfg); err != nil {
		return nil, errors.Errorf("init persistent failed: %v", err)
	}
	return ps, nil
}

// MigrateBetween 保留为向后兼容入口；实际实现在 repository.MigrateBetween。
// 移到 repository 包是为了避免 repository/sqlite 启动期迁移反向 import 本包。
func MigrateBetween(src, dst repository.PersistentMgr) error {
	return repository.MigrateBetween(src, dst)
}

func MigrateHandle(conf string) {
	config, err := ParseConfig(conf)
	if err != nil {
		fmt.Printf("parse config file %s failed: %v\n", conf, err)
		return
	}
	src, err := PersistentCheck(config, config.Source)
	if err != nil {
		fmt.Printf("source [%s] err: %v\n", config.Source, err)
		return
	}
	dst, err := PersistentCheck(config, config.Target)
	if err != nil {
		fmt.Printf("target [%s] err: %v\n", config.Target, err)
		return
	}
	if err := MigrateBetween(src, dst); err != nil {
		fmt.Printf("migrate failed: %v\n", err)
		return
	}
	fmt.Printf("From [%s] migrate to [%s] success!\n", config.Source, config.Target)
}
