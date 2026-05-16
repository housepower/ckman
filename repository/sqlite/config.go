package sqlite

import (
	"encoding/json"
	"fmt"
	"path"

	sqlitedriver "github.com/glebarez/sqlite"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// LocalConfig 字段名与旧 repository/local 完全一致，
// 用户 ckman.hjson 中 persistent_config.local 节点反序列化无需修改。
//
// Format / ConfigDir / ConfigFile 在 SQLite 后端中仅用于：
//   - ConfigDir + ConfigFile.db: SQLite 文件路径
//   - Format + ConfigDir + ConfigFile.<ext>: 启动时定位需迁移的旧 JSON/YAML 文件
type LocalConfig struct {
	Format     string `yaml:"format" json:"format"`
	ConfigDir  string `yaml:"config_dir" json:"config_dir"`
	ConfigFile string `yaml:"config_file" json:"config_file"`
}

// Normalize 填充默认值。
//   - ConfigDir 默认 <work>/conf
//   - ConfigFile 默认 "clusters"（最终落到 clusters.db）
//   - Format 仅迁移时用，默认 "" 表示自动嗅探 .json / .yaml
func (loc *LocalConfig) Normalize() {
	loc.ConfigDir = common.GetStringwithDefault(loc.ConfigDir, path.Join(config.GetWorkDirectory(), "conf"))
	loc.ConfigFile = common.GetStringwithDefault(loc.ConfigFile, "clusters")
}

// DBPath 返回 SQLite 数据库文件绝对路径。
func (loc *LocalConfig) DBPath() string {
	return path.Join(loc.ConfigDir, fmt.Sprintf("%s.db", loc.ConfigFile))
}

// LegacyJSONPath 返回旧 JSON 文件路径（迁移用）。
func (loc *LocalConfig) LegacyJSONPath() string {
	return path.Join(loc.ConfigDir, fmt.Sprintf("%s.json", loc.ConfigFile))
}

// LegacyYAMLPath 返回旧 YAML 文件路径（迁移用）。
func (loc *LocalConfig) LegacyYAMLPath() string {
	return path.Join(loc.ConfigDir, fmt.Sprintf("%s.yaml", loc.ConfigFile))
}

// BuildDialector 把 ckman.hjson 中 persistent_config.local 节点转换为 GORM Dialector。
// 用法：sqlcli 等工具想拿原始 *gorm.DB 而不触发 Init() 的迁移路径时，调此函数 + gorm.Open。
// 入参 cfgMap 取自 config.GlobalConfig.PersistentConfig["local"]。
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
	cfg := LocalConfig{}
	data, err := json.Marshal(cfgMap)
	if err != nil {
		return nil, errors.Wrap(err, "marshal cfgMap")
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshal cfgMap")
	}
	cfg.Normalize()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", cfg.DBPath())
	return sqlitedriver.Open(dsn), nil
}
