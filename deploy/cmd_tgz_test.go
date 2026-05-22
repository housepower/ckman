package deploy

import (
	"strings"
	"testing"
)

func TestGuardCwd(t *testing.T) {
	cases := []struct {
		cwd     string
		guarded bool
	}{
		{"", true},
		{"/", true},
		{"relative/path", true},
		{"./foo", true},
		{"/home/eoi/clickhouse/", false},
		{"/opt/ck/", false},
	}
	for _, c := range cases {
		got := guardCwd(c.cwd)
		if (got != "") != c.guarded {
			t.Errorf("guardCwd(%q) = %q, want guarded=%v", c.cwd, got, c.guarded)
		}
	}
}

func TestTgzUnsafeCwdNeverWipesSystem(t *testing.T) {
	p := &TgzPkg{}
	for _, cwd := range []string{"", "/", "relative"} {
		pkgs := Packages{Cwd: cwd}

		uninstall := p.Uninstall(CkSvrName, pkgs, "1.0")
		if !strings.Contains(uninstall, "exit 1") {
			t.Errorf("Uninstall(cwd=%q) missing abort: %q", cwd, uninstall)
		}
		if strings.Contains(uninstall, "rm -rf /bin") ||
			strings.Contains(uninstall, "rm -rf /etc") {
			t.Errorf("Uninstall(cwd=%q) would wipe system: %q", cwd, uninstall)
		}

		install := p.InstallCmd(CkSvrName, pkgs)
		if !strings.Contains(install, "exit 1") {
			t.Errorf("InstallCmd(cwd=%q) missing abort: %q", cwd, install)
		}

		stop := p.StopCmd(CkSvrName, cwd, "/data")
		if !strings.Contains(stop, "exit 1") {
			t.Errorf("StopCmd(cwd=%q) missing abort: %q", cwd, stop)
		}

		start := p.StartCmd(CkSvrName, cwd, "/data")
		if !strings.Contains(start, "exit 1") {
			t.Errorf("StartCmd(cwd=%q) missing abort: %q", cwd, start)
		}
	}
}
