package deploy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTgzPkg_StartCmd(t *testing.T) {
	p := TgzFacotry{}.Create()
	out := p.StartCmd("clickhouse-server", "/home/eoi/clickhouse", "/var/lib/clickhouse")
	expect := `if [ -f /home/eoi/clickhouse/run/clickhouse-server.pid ]; then P=$(cat /home/eoi/clickhouse/run/clickhouse-server.pid 2>/dev/null); if [ -n "$P" ] && kill -0 "$P" 2>/dev/null; then echo "clickhouse-server already running (pid $P)" >&2; exit 1; fi; fi; rm -f /home/eoi/clickhouse/run/clickhouse-server.pid /var/lib/clickhouse/status; /home/eoi/clickhouse/bin/clickhouse-server --config-file=/home/eoi/clickhouse/etc/clickhouse-server/config.xml --pid-file=/home/eoi/clickhouse/run/clickhouse-server.pid --daemon`
	assert.Equal(t, expect, out)
}

func TestTgzPkg_StopCmd(t *testing.T) {
	p := TgzFacotry{}.Create()
	out := p.StopCmd("clickhouse-server", "/home/eoi/clickhouse", "/var/lib/clickhouse")
	expect := `ps -ef |grep /home/eoi/clickhouse/bin/clickhouse-server |grep -v grep |awk '{print $2}' |xargs kill; for i in $(seq 1 30); do ps -ef |grep /home/eoi/clickhouse/bin/clickhouse-server |grep -v grep >/dev/null || break; sleep 1; done; rm -f /home/eoi/clickhouse/run/clickhouse-server.pid /var/lib/clickhouse/status`
	assert.Equal(t, expect, out)
}

func TestTgzPkg_RestartCmd(t *testing.T) {
	p := TgzFacotry{}.Create()
	out := p.RestartCmd("clickhouse-server", "/home/eoi/clickhouse", "/var/lib/clickhouse")
	expect := p.StopCmd("clickhouse-server", "/home/eoi/clickhouse", "/var/lib/clickhouse") + "; " +
		p.StartCmd("clickhouse-server", "/home/eoi/clickhouse", "/var/lib/clickhouse")
	assert.Equal(t, expect, out)
}

func TestTgzPkg_InstallCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
		Cwd: "/home/eoi/clickhouse",
	}
	p := TgzFacotry{}.Create()
	out := p.InstallCmd(CkSvrName, pkgs)
	expect := `mkdir -p /home/eoi/clickhouse/bin /home/eoi/clickhouse/etc/clickhouse-server/config.d /home/eoi/clickhouse/etc/clickhouse-server/users.d /home/eoi/clickhouse/log/clickhouse-server /home/eoi/clickhouse/run;tar -xf /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz -C /tmp;cp -rf /tmp/clickhouse-common-static-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;tar -xf /tmp/clickhouse-server-22.3.6.5-amd64.tgz -C /tmp;cp -rf /tmp/clickhouse-server-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;cp -rf /tmp/clickhouse-server-22.3.6.5/etc/clickhouse-server /home/eoi/clickhouse/etc/;tar -xf /tmp/clickhouse-client-22.3.6.5-amd64.tgz -C /tmp;cp -rf /tmp/clickhouse-client-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;cp -rf /tmp/clickhouse-client-22.3.6.5/etc/clickhouse-client /home/eoi/clickhouse/etc/`
	assert.Equal(t, expect, out)
}

func TestTgzPkg_UninstallCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
		Cwd: "/home/eoi/clickhouse",
	}
	p := TgzFacotry{}.Create()
	expect := "rm -rf /home/eoi/clickhouse/bin /home/eoi/clickhouse/etc /home/eoi/clickhouse/log /home/eoi/clickhouse/run"
	out := p.Uninstall(CkSvrName, pkgs, "22.3.6.5")
	assert.Equal(t, expect, out)
}

func TestTgzPkg_UpgradeCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
		Cwd: "/home/eoi/clickhouse",
	}
	p := TgzFacotry{}.Create()
	out := p.UpgradeCmd(CkSvrName, pkgs)
	expect := `mkdir -p /home/eoi/clickhouse/bin /home/eoi/clickhouse/etc/clickhouse-server/config.d /home/eoi/clickhouse/etc/clickhouse-server/users.d /home/eoi/clickhouse/log/clickhouse-server /home/eoi/clickhouse/run;tar -xf /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz -C /tmp;cp -rf /tmp/clickhouse-common-static-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;tar -xf /tmp/clickhouse-server-22.3.6.5-amd64.tgz -C /tmp;cp -rf /tmp/clickhouse-server-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;cp -rf /tmp/clickhouse-server-22.3.6.5/etc/clickhouse-server /home/eoi/clickhouse/etc/;tar -xf /tmp/clickhouse-client-22.3.6.5-amd64.tgz -C /tmp;cp -rf /tmp/clickhouse-client-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;cp -rf /tmp/clickhouse-client-22.3.6.5/etc/clickhouse-client /home/eoi/clickhouse/etc/`
	assert.Equal(t, expect, out)
}

func TestRpmPkg_StartCmd(t *testing.T) {
	p := RpmFacotry{}.Create()
	out := p.StartCmd("clickhouse-server", "", "")
	expect := "systemctl start clickhouse-server"
	assert.Equal(t, expect, out)
}

func TestRpmPkg_StopCmd(t *testing.T) {
	p := RpmFacotry{}.Create()
	out := p.StopCmd("clickhouse-server", "", "")
	expect := "systemctl stop clickhouse-server"
	assert.Equal(t, expect, out)
}

func TestRpmPkg_RestartCmd(t *testing.T) {
	p := RpmFacotry{}.Create()
	out := p.RestartCmd("clickhouse-server", "", "")
	expect := `systemctl restart clickhouse-server`
	assert.Equal(t, expect, out)
}

func TestRpmPkg_InstallCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
	}
	p := RpmFacotry{}.Create()
	out := p.InstallCmd(CkSvrName, pkgs)
	expect := `DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps -ivh /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz;DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps -ivh /tmp/clickhouse-server-22.3.6.5-amd64.tgz;DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps -ivh /tmp/clickhouse-client-22.3.6.5-amd64.tgz`
	assert.Equal(t, expect, out)
}

func TestRpmPkg_UninstallCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
	}
	p := RpmFacotry{}.Create()
	out := p.Uninstall(CkSvrName, pkgs, "22.3.6.5")
	expect := `rpm -e $(rpm -qa |grep clickhouse |grep 22.3.6.5)`
	assert.Equal(t, expect, out)
}

func TestRpmPkg_UpgradeCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
	}
	p := RpmFacotry{}.Create()
	out := p.UpgradeCmd(CkSvrName, pkgs)
	expect := `DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps -Uvh /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz;DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps -Uvh /tmp/clickhouse-server-22.3.6.5-amd64.tgz;DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps -Uvh /tmp/clickhouse-client-22.3.6.5-amd64.tgz`
	assert.Equal(t, expect, out)
}

func TestDebPkg_StartCmd(t *testing.T) {
	p := DebFacotry{}.Create()
	out := p.StartCmd("clickhouse-server", "/home/eoi/clickhouse", "")
	expect := "service clickhouse-server start"
	assert.Equal(t, expect, out)
}

func TestDebPkg_StopCmd(t *testing.T) {
	p := DebFacotry{}.Create()
	out := p.StopCmd("clickhouse-server", "/home/eoi/clickhouse", "")
	expect := "service clickhouse-server stop"
	assert.Equal(t, expect, out)
}

func TestDebPkg_RestartCmd(t *testing.T) {
	p := DebFacotry{}.Create()
	out := p.RestartCmd("clickhouse-server", "/home/eoi/clickhouse", "")
	expect := `service clickhouse-server restart`
	assert.Equal(t, expect, out)
}

func TestDebPkg_InstallCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
	}
	p := DebFacotry{}.Create()
	out := p.InstallCmd(CkSvrName, pkgs)
	expect := `DEBIAN_FRONTEND=noninteractive dpkg -i /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz /tmp/clickhouse-server-22.3.6.5-amd64.tgz /tmp/clickhouse-client-22.3.6.5-amd64.tgz`
	assert.Equal(t, expect, out)
}

func TestDebPkg_UninstallCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
	}
	p := DebFacotry{}.Create()
	out := p.Uninstall(CkSvrName, pkgs, "22.3.6.5")
	expect := `dpkg -P clickhouse-client clickhouse-common-static clickhouse-server`
	assert.Equal(t, expect, out)
}

func TestDebPkg_UpgradeCmd(t *testing.T) {
	pkgs := Packages{
		PkgLists: []string{
			"clickhouse-common-static-22.3.6.5-amd64.tgz",
			"clickhouse-server-22.3.6.5-amd64.tgz",
			"clickhouse-client-22.3.6.5-amd64.tgz",
		},
		Cwd: "/home/eoi/clickhouse",
	}
	p := DebFacotry{}.Create()
	out := p.UpgradeCmd(CkSvrName, pkgs)
	expect := `DEBIAN_FRONTEND=noninteractive dpkg -i /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz /tmp/clickhouse-server-22.3.6.5-amd64.tgz /tmp/clickhouse-client-22.3.6.5-amd64.tgz`
	assert.Equal(t, expect, out)
}
