package deploy

import (
	"errors"
	"strings"
	"testing"
)

func TestExplainRPMInstallError(t *testing.T) {
	tests := []struct {
		name     string
		pkgType  string
		message  string
		wantHint bool
	}{
		{name: "exit status", pkgType: "aarch64.rpm", message: "scriptlet failed, exit status 132", wantHint: true},
		{name: "English SIGILL", pkgType: "aarch64.rpm", message: "Illegal instruction (core dumped)", wantHint: true},
		{name: "Chinese SIGILL", pkgType: "aarch64.rpm", message: "非法指令 (核心已转储)", wantHint: true},
		{name: "localized scriptlet status", pkgType: "aarch64.rpm", message: "警告：%post(clickhouse-server) 脚本执行失败，退出状态码为 132", wantHint: true},
		{name: "other RPM error", pkgType: "aarch64.rpm", message: "permission denied", wantHint: false},
		{name: "non-RPM package", pkgType: "arm64.tgz", message: "Illegal instruction", wantHint: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := errors.New(test.message)
			got := explainRPMInstallError(test.pkgType, original)
			hasHint := strings.Contains(got.Error(), "SIGILL (exit status 132)")
			if hasHint != test.wantHint {
				t.Fatalf("explainRPMInstallError() = %q, want hint %v", got, test.wantHint)
			}
		})
	}
}
