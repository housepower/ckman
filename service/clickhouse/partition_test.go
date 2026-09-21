package clickhouse

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestBuildFixPartCmdsSkipsHealthyParts(t *testing.T) {
	// 正常 DETACH 出来的 part，reason 为空，无需搬运，不应该生成任何命令
	parts := []DetachedInfo{
		{PartitionId: "20260908", Name: "20260908_1_1_0", Disk: "default", Reason: ""},
	}
	dataPaths := []string{"/var/lib/clickhouse/data/default/events/"}

	if cmds := buildFixPartCmds(parts, dataPaths); len(cmds) != 0 {
		t.Fatalf("healthy part should produce no cmds, got %v", cmds)
	}
}

func TestBuildFixPartCmdsRenamesBrokenParts(t *testing.T) {
	parts := []DetachedInfo{
		{PartitionId: "20260908", Name: "broken_20260908_1_1_0", Disk: "default", Reason: "broken"},
		{PartitionId: "20260908", Name: "ignored_20260908_2_2_0", Disk: "hdd", Reason: "ignored"},
	}
	dataPaths := []string{
		"/var/lib/clickhouse/data/default/events/",
		"/var/lib/clickhouse/disks/hdd/data/default/events/",
	}

	cmds := buildFixPartCmds(parts, dataPaths)
	if len(cmds) != 2 {
		t.Fatalf("got %d cmds, want 2: %v", len(cmds), cmds)
	}
	want := "mv /var/lib/clickhouse/data/default/events/detached/broken_20260908_1_1_0 " +
		"/var/lib/clickhouse/data/default/events/detached/20260908_1_1_0; " +
		"chown -R clickhouse:clickhouse /var/lib/clickhouse/data/default/events/detached/20260908_1_1_0"
	if cmds[0] != want {
		t.Fatalf("cmds[0] =\n%s\nwant\n%s", cmds[0], want)
	}
	if !strings.Contains(cmds[1], "/disks/hdd/data/default/events/detached/20260908_2_2_0") {
		t.Fatalf("cmds[1] should target the hdd disk path, got %s", cmds[1])
	}
}

func TestExecFixCmdsNoopOnEmpty(t *testing.T) {
	// 没有命令要跑时必须直接返回，绝不能把空串丢给 RemoteExecute:
	// genFinalScript 会拼出 "sudo "，sudo 无参数打印 usage 并以 1 退出。
	// 这里的 host 不可达，一旦真的发起 SSH 就会返回错误。
	conf := &model.CKManClickHouseConfig{
		SshUser:  "ckman",
		SshPort:  22,
		NeedSudo: true,
	}
	if err := execFixCmds(conf, "192.0.2.1", nil); err != nil {
		t.Fatalf("empty cmds should be a no-op, got %v", err)
	}
}
