package config

import (
	"os"
	"path/filepath"
	"testing"
)

// 首次解析时 ConfigFile 还没写进 GlobalConfig，fillDefault 因此把默认路径
// 算成了进程 cwd 的父目录（systemd 下是 "/"），安装包列表于是去
// /package/clickhouse 里找，永远为空。默认路径必须始终以配置文件位置为准。
func TestParseConfigFileDefaultsUseConfigDirectory(t *testing.T) {
	savedConfig, savedWarnings := GlobalConfig, ConfigWarnings
	defer func() { GlobalConfig, ConfigWarnings = savedConfig, savedWarnings }()

	root := t.TempDir()
	confDir := filepath.Join(root, "conf")
	if err := os.MkdirAll(confDir, 0755); err != nil {
		t.Fatalf("mkdir conf: %v", err)
	}
	confFile := filepath.Join(confDir, "ckman.hjson")
	if err := os.WriteFile(confFile, []byte("{}"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := ParseConfigFile(confFile, "test"); err != nil {
		t.Fatalf("ParseConfigFile: %v", err)
	}

	if GlobalConfig.Server.PkgPath != root {
		t.Errorf("Server.PkgPath = %q, want %q", GlobalConfig.Server.PkgPath, root)
	}
	if want := filepath.Join(root, "conf", "server.crt"); GlobalConfig.Server.CertFile != want {
		t.Errorf("Server.CertFile = %q, want %q", GlobalConfig.Server.CertFile, want)
	}
	if want := filepath.Join(root, "conf", "server.key"); GlobalConfig.Server.KeyFile != want {
		t.Errorf("Server.KeyFile = %q, want %q", GlobalConfig.Server.KeyFile, want)
	}
}
