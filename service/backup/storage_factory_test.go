package backup_test

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/backup"
	"github.com/housepower/ckman/service/backup/storage"
)

func TestStorageFactory_S3(t *testing.T) {
	policy := model.BackupPolicy{
		TargetType: model.BACKUP_S3,
		S3: model.TargetS3{
			Endpoint: "http://s3.example.com:9000", Bucket: "b",
			AccessKeyID: "a", SecretAccessKey: "s",
		},
	}
	st := backup.NewStorageForPolicy(policy, model.CKManClickHouseConfig{})
	if _, ok := st.(*storage.S3); !ok {
		t.Fatalf("expected *storage.S3, got %T", st)
	}
}

func TestStorageFactory_Local(t *testing.T) {
	policy := model.BackupPolicy{
		TargetType: model.BACKUP_LOCAL,
		Local:      model.TargetLocal{Path: "/data/backup"},
	}
	cluster := model.CKManClickHouseConfig{
		SshUser: "ck", SshPassword: "p", SshPort: 22,
	}
	st := backup.NewStorageForPolicy(policy, cluster)
	if _, ok := st.(*storage.Local); !ok {
		t.Fatalf("expected *storage.Local, got %T", st)
	}
}

func TestStorageFactory_UnknownTargetReturnsNil(t *testing.T) {
	policy := model.BackupPolicy{TargetType: "unknown"}
	if backup.NewStorageForPolicy(policy, model.CKManClickHouseConfig{}) != nil {
		t.Fatal("expected nil for unknown target type")
	}
}
