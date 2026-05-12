package backup

import (
	"errors"
	"testing"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func TestClickHouseAdapter_ConnFactory_SkipsUnreachableReplica(t *testing.T) {
	dialed := []string{}
	dial := func(host string, _ model.ConnetOption) (*common.Conn, error) {
		dialed = append(dialed, host)
		if host == "h1-bad" {
			return nil, errors.New("conn refused")
		}
		return nil, nil // 测试 fake 不需要真 conn
	}
	cluster := model.CKManClickHouseConfig{
		Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "h1-bad"}, {Ip: "h1-good"}}},
			{Replicas: []model.CkReplica{{Ip: "h2-good"}}},
		},
	}
	a := &ClickHouseAdapter{
		getCluster: func(string) (model.CKManClickHouseConfig, error) { return cluster, nil },
		dial:       dial,
	}
	conns, err := a.ConnFactory("ckA")
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 || conns[0].host != "h1-good" || conns[1].host != "h2-good" {
		t.Fatalf("conns: %+v", conns)
	}
	// 一定有 dial 过 h1-bad
	found := false
	for _, h := range dialed {
		if h == "h1-bad" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("h1-bad should be dialed and skipped")
	}
}

func TestClickHouseAdapter_ConnFactory_FailsWhenAllReplicasDown(t *testing.T) {
	dial := func(string, model.ConnetOption) (*common.Conn, error) {
		return nil, errors.New("conn refused")
	}
	cluster := model.CKManClickHouseConfig{
		Shards: []model.CkShard{{Replicas: []model.CkReplica{{Ip: "h1"}, {Ip: "h2"}}}},
	}
	a := &ClickHouseAdapter{
		getCluster: func(string) (model.CKManClickHouseConfig, error) { return cluster, nil },
		dial:       dial,
	}
	if _, err := a.ConnFactory("ckA"); err == nil {
		t.Fatal("expected error when all replicas down")
	}
}

func TestClickHouseAdapter_ConnFactory_BubbleUpClusterError(t *testing.T) {
	a := &ClickHouseAdapter{
		getCluster: func(string) (model.CKManClickHouseConfig, error) {
			return model.CKManClickHouseConfig{}, errors.New("not found")
		},
	}
	if _, err := a.ConnFactory("ckA"); err == nil {
		t.Fatal("expected cluster error to bubble up")
	}
}

// CollectChecksumOnHost 测试：fake sshExec
func TestClickHouseAdapter_CollectChecksumOnHost_PopulatesPathInfo(t *testing.T) {
	cluster := model.CKManClickHouseConfig{
		SshUser: "ck", SshPort: 22,
	}
	sshOut := `d41d8cd98f00b204e9800998ecf8427e  /var/lib/clickhouse/data/dba/t1/all_1_1_0/data.bin
098f6bcd4621d373cade4e832627b4f6  /var/lib/clickhouse/data/dba/t1/all_1_1_0/columns.txt
`
	a := &ClickHouseAdapter{
		getCluster: func(string) (model.CKManClickHouseConfig, error) { return cluster, nil },
		sshExec:    func(common.SshOptions, string) (string, error) { return sshOut, nil },
		queryPartitionPaths: func(_ *shardConn, _, _, _ string) ([]string, error) {
			return []string{"/var/lib/clickhouse/data/dba/t1/all_1_1_0"}, nil
		},
	}
	run := &model.BackupRun{
		ClusterName: "ckA", Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	if err := a.CollectChecksumOnHost(&shardConn{host: "h1"}, run); err != nil {
		t.Fatal(err)
	}
	pi := run.Partitions[0].PathInfo
	if len(pi) != 2 {
		t.Fatalf("expected 2 PathInfo entries, got %d: %+v", len(pi), pi)
	}
	wantRPaths := map[string]string{
		"20250508/dba.t1/h1/data/dba/t1/all_1_1_0/data.bin":    "d41d8cd98f00b204e9800998ecf8427e",
		"20250508/dba.t1/h1/data/dba/t1/all_1_1_0/columns.txt": "098f6bcd4621d373cade4e832627b4f6",
	}
	for rpath, wantMD5 := range wantRPaths {
		info, ok := pi[rpath]
		if !ok {
			t.Errorf("missing PathInfo for RPath=%s; got keys=%v", rpath, mapKeys(pi))
			continue
		}
		if info.RPath != rpath {
			t.Errorf("RPath field mismatch: key=%s, info.RPath=%s", rpath, info.RPath)
		}
		if info.MD5 != wantMD5 {
			t.Errorf("MD5 mismatch for %s: got %s, want %s", rpath, info.MD5, wantMD5)
		}
		if info.Host != "h1" || info.LPath == "" {
			t.Errorf("incomplete: %+v", info)
		}
	}
	if run.Partitions[0].FileNum != 2 {
		t.Errorf("expected FileNum=2, got %d", run.Partitions[0].FileNum)
	}
}

func mapKeys(m map[string]model.PathInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestClickHouseAdapter_CollectChecksumOnHost_SkipsNonWaiting(t *testing.T) {
	a := &ClickHouseAdapter{
		getCluster: func(string) (model.CKManClickHouseConfig, error) { return model.CKManClickHouseConfig{}, nil },
		sshExec: func(common.SshOptions, string) (string, error) {
			t.Fatal("sshExec should not be called for non-waiting partitions")
			return "", nil
		},
	}
	run := &model.BackupRun{
		Partitions: []model.BackupRunPartition{
			{Partition: "p1", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	_ = a.CollectChecksumOnHost(&shardConn{host: "h1"}, run)
}
