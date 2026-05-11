package backup

import (
	"testing"

	"github.com/housepower/ckman/model"
)

func TestFilterLocalDisks_FiltersLocalOnly(t *testing.T) {
	cc := model.CKManClickHouseConfig{
		Storage: &model.Storage{Disks: []model.Disk{
			{Name: "cold", Type: "local", AllowedBackup: true, DiskLocal: &model.DiskLocal{Path: "/data/cold"}},
			{Name: "hdfs1", Type: "hdfs", AllowedBackup: true, DiskHdfs: &model.DiskHdfs{Endpoint: "hdfs://localhost:8020/"}},
			{Name: "s3-1", Type: "s3", AllowedBackup: false, DiskS3: &model.DiskS3{}},
		}},
	}
	got := filterLocalDisks(cc)
	if len(got) != 1 || got[0].Name != "cold" || got[0].Path != "/data/cold" {
		t.Fatalf("got %+v", got)
	}
}

func TestFilterLocalDisks_NilStorageReturnsEmpty(t *testing.T) {
	got := filterLocalDisks(model.CKManClickHouseConfig{})
	if got == nil || len(got) != 0 {
		t.Fatalf("expected non-nil empty slice, got %+v", got)
	}
}

func TestFilterLocalDisks_PreservesAllowedBackupFlag(t *testing.T) {
	cc := model.CKManClickHouseConfig{
		Storage: &model.Storage{Disks: []model.Disk{
			{Name: "a", Type: "local", AllowedBackup: true, DiskLocal: &model.DiskLocal{Path: "/a"}},
			{Name: "b", Type: "local", AllowedBackup: false, DiskLocal: &model.DiskLocal{Path: "/b"}},
		}},
	}
	got := filterLocalDisks(cc)
	if len(got) != 2 || got[0].AllowedBackup != true || got[1].AllowedBackup != false {
		t.Fatalf("got %+v", got)
	}
}

func TestFilterLocalDisks_SkipsDiskLocalNil(t *testing.T) {
	// type=local but DiskLocal==nil (malformed config) should be skipped
	cc := model.CKManClickHouseConfig{
		Storage: &model.Storage{Disks: []model.Disk{
			{Name: "bad", Type: "local", DiskLocal: nil},
			{Name: "good", Type: "local", AllowedBackup: true, DiskLocal: &model.DiskLocal{Path: "/good"}},
		}},
	}
	got := filterLocalDisks(cc)
	if len(got) != 1 || got[0].Name != "good" {
		t.Fatalf("got %+v", got)
	}
}
