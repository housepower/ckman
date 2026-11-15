package diagnosis

import "fmt"

const (
	SceneReplicaStatus    = "replica_status"
	SceneTableHealth      = "table_health"
	SceneResourcePressure = "resource_pressure"
	SceneSlowQuery        = "slow_query"
	SceneDistributedDDL   = "distributed_ddl"
)

type Profile struct {
	Scene            string   `json:"scene"`
	Description      string   `json:"description"`
	Checks           []string `json:"checks"`
	RequiresTable    bool     `json:"requires_table"`
	RequiresQueryID  bool     `json:"requires_query_id"`
	RequiresDDLEntry bool     `json:"requires_ddl_entry"`
}

var profiles = []Profile{
	{Scene: SceneReplicaStatus, Description: "指定表复制状态诊断", Checks: []string{"replica_status", "replication_queue_failures", "replication_queue_backlog"}, RequiresTable: true},
	{Scene: SceneTableHealth, Description: "指定表健康诊断", Checks: []string{"too_many_parts", "detached_parts", "mutations_stuck"}, RequiresTable: true},
	{Scene: SceneResourcePressure, Description: "集群资源异常诊断", Checks: []string{"resource_pressure", "disk_space_low"}},
	{Scene: SceneSlowQuery, Description: "指定慢查询诊断", Checks: []string{"slow_query"}, RequiresQueryID: true},
	{Scene: SceneDistributedDDL, Description: "指定分布式 DDL 诊断", Checks: []string{"distributed_ddl"}, RequiresDDLEntry: true},
}

func ListProfiles() []Profile {
	out := make([]Profile, len(profiles))
	for i, profile := range profiles {
		out[i] = profile
		out[i].Checks = append([]string(nil), profile.Checks...)
	}
	return out
}

func ResolveProfile(scene string) (Profile, error) {
	for _, profile := range profiles {
		if profile.Scene == scene {
			profile.Checks = append([]string(nil), profile.Checks...)
			return profile, nil
		}
	}
	return Profile{}, fmt.Errorf("diagnosis: unknown scene %q", scene)
}
