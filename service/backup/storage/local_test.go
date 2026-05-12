package storage

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func TestLocal_Init_RejectsBadPath(t *testing.T) {
	for _, bad := range []string{"", "rel/path", "/data/../etc", "/data;rm -rf /", "/data $a"} {
		l := NewLocal(model.TargetLocal{Path: bad}, "", nil)
		if err := l.Init(); err == nil {
			t.Errorf("path %q should reject", bad)
		}
	}
}

func TestLocal_Init_AcceptsGoodPath(t *testing.T) {
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, "", func(string) common.SshOptions { return common.SshOptions{} })
	if err := l.Init(); err != nil {
		t.Errorf("good path should pass: %v", err)
	}
}

func TestLocal_BackupSQL(t *testing.T) {
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, "", nil)
	out := l.BackupSQL("dba", "events_log", "20250508", "20250508/dba.events_log/host1")
	if !strings.Contains(out, "TO File('/data/backups/20250508/dba.events_log/host1')") {
		t.Fatalf("unexpected: %s", out)
	}
	if !strings.Contains(out, "PARTITION '20250508'") {
		t.Fatalf("missing partition: %s", out)
	}
}

func TestLocal_BackupSQL_AllPartition(t *testing.T) {
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, "", nil)
	out := l.BackupSQL("d", "t", "all", "all/d.t/h")
	if strings.Contains(out, "PARTITION") {
		t.Fatalf("'all' should not emit PARTITION clause: %s", out)
	}
	if !strings.Contains(out, "TO File(") {
		t.Fatalf("missing TO File: %s", out)
	}
}

func TestLocal_RestoreSQL(t *testing.T) {
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, "", nil)
	out := l.RestoreSQL("d", "t", "20250508", "20250508/d.t/h")
	if !strings.Contains(out, "FROM File('/data/backups/20250508/d.t/h')") || !strings.Contains(out, "PARTITION '20250508'") {
		t.Fatalf("unexpected: %s", out)
	}
}

func TestLocal_CleanPartition_RejectsBadIdentifier(t *testing.T) {
	// 即便 sshOpts 注入正常，identifier 中带反引号 / 引号也应被拒
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, "",
		func(string) common.SshOptions { return common.SshOptions{} })
	if err := l.Init(); err != nil {
		t.Fatal(err)
	}
	err := l.CleanPartition("bad`db", "t", "h", "20250508")
	if err == nil {
		t.Fatal("identifier with backtick should reject")
	}
}

func TestLocal_CleanPartition_NoSshOpts(t *testing.T) {
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, "", nil)
	_ = l.Init()
	if err := l.CleanPartition("d", "t", "h", "p"); err == nil {
		t.Fatal("CleanPartition without sshOpts should error")
	}
}

func TestLocal_Type(t *testing.T) {
	l := NewLocal(model.TargetLocal{}, "", nil)
	if l.Type() != model.BACKUP_LOCAL {
		t.Fatalf("Type: %s", l.Type())
	}
}
