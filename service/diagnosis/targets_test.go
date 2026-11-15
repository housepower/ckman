package diagnosis

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func unusedConnect(context.Context, string, string, model.ConnetOption) (*common.Conn, error) {
	panic("target construction must not connect")
}

func TestBuildTargetsUsesEveryReplicaAndStableShardIdentity(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		Cluster:  "prod",
		Protocol: clickhouse.Native.String(),
		Port:     9000,
		Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "10.0.0.1"}, {Ip: "10.0.0.2"}}},
			{Replicas: []model.CkReplica{{Ip: "10.0.0.3"}, {Ip: "2001:db8::1"}}},
		},
	}

	targets, err := buildTargets(conf, unusedConnect)
	if err != nil {
		t.Fatalf("buildTargets: %v", err)
	}
	if len(targets) != 4 {
		t.Fatalf("targets = %d, want 4", len(targets))
	}
	wantNode := []string{"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000", "[2001:db8::1]:9000"}
	wantShard := []string{"prod/shard-1", "prod/shard-1", "prod/shard-2", "prod/shard-2"}
	for i := range targets {
		if targets[i].NodeID != wantNode[i] || targets[i].ShardID != wantShard[i] || targets[i].Querier == nil {
			t.Errorf("targets[%d] = %+v, want node=%s shard=%s non-nil querier", i, targets[i], wantNode[i], wantShard[i])
		}
	}
}

func TestBuildTargetsUsesHTTPPort(t *testing.T) {
	conf := model.CKManClickHouseConfig{Cluster: "c", Protocol: clickhouse.HTTP.String(), HttpPort: 8443, Secure: true, Shards: []model.CkShard{{Replicas: []model.CkReplica{{Ip: "host-a"}}}}}
	targets, err := buildTargets(conf, unusedConnect)
	if err != nil {
		t.Fatal(err)
	}
	if targets[0].NodeID != "host-a:8443" {
		t.Fatalf("NodeID = %q, want host-a:8443", targets[0].NodeID)
	}
}

func TestBuildTargetsRejectsIncompleteOrDuplicateTopology(t *testing.T) {
	cases := []model.CKManClickHouseConfig{
		{Cluster: "empty"},
		{Cluster: "empty-shard", Shards: []model.CkShard{{}}},
		{Cluster: "empty-host", Shards: []model.CkShard{{Replicas: []model.CkReplica{{Ip: " "}}}}},
		{Cluster: "duplicate", Port: 9000, Shards: []model.CkShard{{Replicas: []model.CkReplica{{Ip: "h"}}}, {Replicas: []model.CkReplica{{Ip: "h"}}}}},
	}
	for _, conf := range cases {
		if _, err := buildTargets(conf, unusedConnect); err == nil {
			t.Errorf("cluster %q: expected error", conf.Cluster)
		}
	}
}

func TestListProfilesReturnsCopy(t *testing.T) {
	got := ListProfiles()
	got[0].Checks[0] = "mutated"
	again, err := ResolveProfile(SceneReplicaStatus)
	if err != nil {
		t.Fatal(err)
	}
	if again.Checks[0] == "mutated" {
		t.Fatal("ListProfiles leaked registry backing slice")
	}
}

func TestBusinessProfilesDeclareRequiredContext(t *testing.T) {
	profiles := ListProfiles()
	if len(profiles) != 5 {
		t.Fatalf("profiles = %d, want 5", len(profiles))
	}
	want := map[string][]string{
		SceneReplicaStatus: {"replica_status", "replication_queue_failures", "replication_queue_backlog"},
		SceneTableHealth:   {"too_many_parts", "detached_parts", "mutations_stuck"},
	}
	for _, profile := range profiles {
		checks, ok := want[profile.Scene]
		if !ok {
			continue
		}
		if !profile.RequiresTable || len(checks) != len(profile.Checks) {
			t.Errorf("unexpected table profile: %+v", profile)
		}
		for i := range checks {
			if profile.Checks[i] != checks[i] {
				t.Errorf("profile %s checks = %v, want %v", profile.Scene, profile.Checks, checks)
			}
		}
	}
	slow, _ := ResolveProfile(SceneSlowQuery)
	ddl, _ := ResolveProfile(SceneDistributedDDL)
	resource, _ := ResolveProfile(SceneResourcePressure)
	if !slow.RequiresQueryID || !ddl.RequiresDDLEntry || resource.RequiresTable || resource.RequiresQueryID || resource.RequiresDDLEntry {
		t.Fatalf("profile context requirements are incorrect: slow=%+v ddl=%+v resource=%+v", slow, ddl, resource)
	}
}
