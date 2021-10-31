package local

import (
	"fmt"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"path"
)

type LocalConfig struct {
	Format     string `yaml:"format" json:"format"`
	ConfigDir  string `yaml:"config_dir" json:"config_dir"`
	ConfigFile string `yaml:"config_file" json:"config_file"`
}

var FormatFileSuffix = map[string]string{
	FORMAT_JSON: "json",
	FORMAT_YAML: "yaml",
}

func (loc *LocalConfig) Normalize() {
	loc.Format = common.GetStringwithDefault(loc.Format, FORMAT_JSON)
	loc.ConfigDir = common.GetStringwithDefault(loc.ConfigDir, path.Join(config.GetWorkDirectory(), ClickHouseClusterDir))
	loc.ConfigFile = fmt.Sprintf("%s.%s", common.GetStringwithDefault(loc.ConfigFile, ClickHouseClustersFile), FormatFileSuffix[loc.Format])
}
