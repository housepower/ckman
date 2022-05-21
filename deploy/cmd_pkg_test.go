package deploy

import (
	"fmt"
	"testing"
)

func TestTgzPkg_StartCmd(t *testing.T) {
	p := TgzPkg{}
	fmt.Println(p.StartCmd("clickhouse-server", "/home/eoi/clickhouse"))
	// /home/eoi/clickhouse/bin/clickhouse-server --config-file=/home/eoi/clickhouse/etc/clickhouse-server/config.xml --pid-file=/home/eoi/clickhouse/run/clickhouse-server.pid --daemon
}

func TestTgzPkg_StopCmd(t *testing.T) {
	p := TgzPkg{}
	fmt.Println(p.StopCmd("clickhouse-server", "/home/eoi/clickhouse"))
	// ps -ef |grep /home/eoi/clickhouse/bin/clickhouse-server |grep -v grep |awk '{print $2}' |xargs kill
}

func TestTgzPkg_RestartCmd(t *testing.T) {
	p := &TgzPkg{}
	fmt.Println(p.RestartCmd("clickhouse-server", "/home/eoi/clickhouse"))
	/*
	ps -ef |grep /home/eoi/clickhouse/bin/clickhouse-server |grep -v grep |awk '{print $2}' |xargs kill;
	/home/eoi/clickhouse/bin/clickhouse-server --config-file=/home/eoi/clickhouse/etc/clickhouse-server/config.xml --pid-file=/home/eoi/clickhouse/run/clickhouse-server.pid --daemon
	*/
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
	p := &TgzPkg{}
	fmt.Println(p.InstallCmd(pkgs))
	/*
	mkdir -p /home/eoi/clickhouse/bin /home/eoi/clickhouse/etc /home/eoi/clickhouse/log/clickhouse-server /home/eoi/clickhouse/run /home/eoi/clickhouse/data/clickhouse;
	tar -xvf /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz;
	cp -rf /tmp/clickhouse-common-static-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;
	tar -xvf /tmp/clickhouse-server-22.3.6.5-amd64.tgz;
	cp -rf /tmp/clickhouse-server-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;
	cp -rf /tmp/clickhouse-server-22.3.6.5/etc/clickhouse-* /home/eoi/clickhouse/etc;
	tar -xvf /tmp/clickhouse-client-22.3.6.5-amd64.tgz;
	cp -rf /tmp/clickhouse-client-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;
	cp -rf /tmp/clickhouse-client-22.3.6.5/etc/clickhouse-* /home/eoi/clickhouse/etc
	*/
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
	p := &TgzPkg{}
	fmt.Println(p.Uninstall(pkgs))
	// rm -rf /home/eoi/clickhouse
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
	p := &TgzPkg{}
	fmt.Println(p.UpgradeCmd(pkgs))
	/*
		mkdir -p /home/eoi/clickhouse/bin /home/eoi/clickhouse/etc /home/eoi/clickhouse/log/clickhouse-server /home/eoi/clickhouse/run /home/eoi/clickhouse/data/clickhouse;
		tar -xvf /tmp/clickhouse-common-static-22.3.6.5-amd64.tgz;
		cp -rf /tmp/clickhouse-common-static-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;
		tar -xvf /tmp/clickhouse-server-22.3.6.5-amd64.tgz;
		cp -rf /tmp/clickhouse-server-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;
		cp -rf /tmp/clickhouse-server-22.3.6.5/etc/clickhouse-* /home/eoi/clickhouse/etc;
		tar -xvf /tmp/clickhouse-client-22.3.6.5-amd64.tgz;
		cp -rf /tmp/clickhouse-client-22.3.6.5/usr/bin/* /home/eoi/clickhouse/bin;
		cp -rf /tmp/clickhouse-client-22.3.6.5/etc/clickhouse-* /home/eoi/clickhouse/etc
	*/
}