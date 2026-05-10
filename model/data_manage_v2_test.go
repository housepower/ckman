package model

import (
	"testing"
	"time"
)

func TestBackupPolicy_RoundTrip(t *testing.T) {
	p := BackupPolicy{
		PolicyID:     "p-1",
		ClusterName:  "ckA",
		Database:     "dba",
		Table:        "t1",
		ScheduleType: BACKUP_SCHEDULED,
		Crontab:      "0 3 * * *",
		Instance:     "ckman-01",
		BackupStyle:  BACKUP_STYLE_INCR,
		BackupType:   BACKUP_TYPE_PARTITION,
		DaysBefore:   7,
		Partitions:   []string{"20250508"},
		TargetType:   BACKUP_S3,
		Compression:  "zstd",
		Checksum:     true,
		Clean:        false,
		Enabled:      true,
		Deleted:      false,
		CreateTime:   time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
		UpdateTime:   time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	}
	bs, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got BackupPolicy
	if err := json.Unmarshal(bs, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.PolicyID != p.PolicyID || got.Crontab != p.Crontab || len(got.Partitions) != 1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestBackupRun_PartitionEmbedded(t *testing.T) {
	r := BackupRun{
		RunID:    "r-1",
		PolicyID: "p-1",
		Status:   BACKUP_STATUS_SUCCESS,
		Partitions: []BackupRunPartition{
			{Partition: "20250508", Status: BACKUP_PARTITION_STATUS_SUCCESS, Size: 1234},
		},
	}
	bs, _ := json.Marshal(r)
	var got BackupRun
	if err := json.Unmarshal(bs, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Partitions) != 1 || got.Partitions[0].Size != 1234 {
		t.Fatalf("partitions not embedded correctly: %+v", got)
	}
}
