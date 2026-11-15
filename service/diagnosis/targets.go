package diagnosis

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

// BuildTargets converts CKMAN's topology into one diagnosis target per
// replica. It deliberately does not Ping while building the list: an
// unreachable node must remain visible and be reported as query_failed by the
// diagnosis library rather than silently disappearing from coverage.
func BuildTargets(conf model.CKManClickHouseConfig) ([]diagnose.Target, error) {
	return buildTargets(conf, common.ConnectClickHouseWithCtx)
}

func buildTargets(conf model.CKManClickHouseConfig, connect connectFunc) ([]diagnose.Target, error) {
	if connect == nil {
		return nil, fmt.Errorf("diagnosis: connect function is nil")
	}
	conf.Normalize()
	if len(conf.Shards) == 0 {
		return nil, fmt.Errorf("diagnosis: cluster %q has no shard topology", conf.Cluster)
	}

	options := conf.GetConnOption()
	targets := make([]diagnose.Target, 0, len(conf.Hosts))
	seen := make(map[string]struct{})
	for shardIndex, shard := range conf.Shards {
		if len(shard.Replicas) == 0 {
			return nil, fmt.Errorf("diagnosis: cluster %q shard %d has no replicas", conf.Cluster, shardIndex+1)
		}
		shardID := fmt.Sprintf("%s/shard-%d", conf.Cluster, shardIndex+1)
		for _, replica := range shard.Replicas {
			host := strings.TrimSpace(replica.Ip)
			if host == "" {
				return nil, fmt.Errorf("diagnosis: cluster %q shard %d contains an empty replica host", conf.Cluster, shardIndex+1)
			}
			nodeID := net.JoinHostPort(host, strconv.Itoa(options.Port))
			if _, exists := seen[nodeID]; exists {
				return nil, fmt.Errorf("diagnosis: duplicate node %q in topology", nodeID)
			}
			seen[nodeID] = struct{}{}
			targets = append(targets, diagnose.Target{
				NodeID:  nodeID,
				ShardID: shardID,
				Querier: &ckQuerier{host: host, options: options, connect: connect},
			})
		}
	}
	return targets, nil
}
