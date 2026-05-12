package storage

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestS3_BackupSQL(t *testing.T) {
	s := NewS3(model.TargetS3{
		Endpoint:        "http://s3.example.com:9000",
		Bucket:          "ckman-backup",
		AccessKeyID:     "AK",
		SecretAccessKey: "SK",
	}, "gzip")
	out := s.BackupSQL("dba", "events_log", "20250508", "20250508/dba.events_log/host1")
	if !strings.Contains(out, "S3('http://s3.example.com:9000/ckman-backup/20250508/dba.events_log/host1', 'AK', 'SK')") {
		t.Fatalf("unexpected sql: %s", out)
	}
	if !strings.Contains(out, "PARTITION '20250508'") {
		t.Fatalf("missing partition: %s", out)
	}
}

func TestS3_BackupSQL_AllPartition(t *testing.T) {
	s := NewS3(model.TargetS3{Endpoint: "http://x", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s"}, "")
	out := s.BackupSQL("d", "t", "all", "all/d.t/h")
	if strings.Contains(out, "PARTITION") {
		t.Fatalf("'all' should not emit PARTITION clause: %s", out)
	}
	if !strings.Contains(out, "TO S3(") {
		t.Fatalf("missing TO S3: %s", out)
	}
}

func TestS3_RestoreSQL(t *testing.T) {
	s := NewS3(model.TargetS3{Endpoint: "http://x", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s"}, "")
	out := s.RestoreSQL("d", "t", "20250508", "20250508/d.t/h")
	if !strings.Contains(out, "FROM S3(") || !strings.Contains(out, "PARTITION '20250508'") {
		t.Fatalf("unexpected: %s", out)
	}
}

func TestS3_Init_RejectsBadEndpoint(t *testing.T) {
	s := NewS3(model.TargetS3{Endpoint: "ftp://nope"}, "")
	if err := s.Init(); err == nil {
		t.Fatal("ftp endpoint should reject")
	}
}

func TestS3_EscapesSingleQuoteInCredentials(t *testing.T) {
	// AK/SK 含单引号必须转义，否则 BACKUP TABLE SQL 解析会断
	s := NewS3(model.TargetS3{Endpoint: "http://x", Bucket: "b", AccessKeyID: "a'b", SecretAccessKey: "s'k"}, "")
	out := s.BackupSQL("d", "t", "p", "p/d.t/h")
	if strings.Count(out, "''") < 2 {
		t.Fatalf("expected ' to be doubled, got: %s", out)
	}
}

func TestS3_Type(t *testing.T) {
	s := NewS3(model.TargetS3{}, "")
	if s.Type() != model.BACKUP_S3 {
		t.Fatalf("Type: %s", s.Type())
	}
}
