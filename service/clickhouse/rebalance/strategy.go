package rebalance

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

// Strategy is the per-table rebalance algorithm. Two implementations are
// provided: ByPartition (move whole partitions across hosts) and
// ByShardingKey (rehash rows via a tmp table).
//
// Validate is the pre-flight: it inspects the table and rejects unsupported
// shapes (e.g. shardingkey not present on the target table) before any work.
// ctrlConn is a control-plane connection (typically to one of the cluster's
// hosts, opened by the orchestrator) for metadata lookups that don't need
// shard-level fan-out. Validate may ignore it if no metadata work is needed.
//
// Plan inspects current cluster state and reports the moves/reshuffle that
// Run would perform if invoked right now. It must not mutate cluster data;
// it may open pooled connections and run read-only queries. Used by the
// rebalance_plan preview endpoint to power confirm-before-execute UIs.
//
// Run executes the full lifecycle for one table.
type Strategy interface {
	Name() string
	Validate(r *Rebalancer, ctrlConn *common.Conn) error
	Plan(r *Rebalancer) (model.TablePlan, error)
	Run(r *Rebalancer) error
}

// PickStrategy chooses the algorithm for one table based on its requested policy.
// Defaults to ByPartition when policy is empty or unrecognized; that matches the
// pre-2a behavior where any non-"shardingkey" value fell through to partition.
func PickStrategy(t model.RebalanceTables) Strategy {
	if t.Policy == model.RebalancePolicyShardingKey {
		return ByShardingKey{}
	}
	return ByPartition{}
}
