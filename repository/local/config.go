package local

import (
	"fmt"
	"github.com/housepower/ckman/common"
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

func (config *LocalConfig) Normalize() {
	config.Format = common.GetStringwithDefault(config.Format, FORMAT_JSON)
	config.ConfigDir = common.GetStringwithDefault(config.ConfigDir, ClickHouseClusterDir)
	config.ConfigFile = fmt.Sprintf("%s.%s", common.GetStringwithDefault(config.ConfigFile, ClickHouseClustersFile), FormatFileSuffix[config.Format])
}
