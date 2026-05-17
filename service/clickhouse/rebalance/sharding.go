package rebalance

import (
	"fmt"

	"github.com/housepower/ckman/model"
)

// ShardingFunc returns the ClickHouse expression that hashes a column value
// into a uint64 used for shard assignment during shardingkey-based rebalance.
// Numeric columns are simply cast; strings are hashed with xxHash64. Nullable
// and Array columns are rejected earlier in getShardingType.
func ShardingFunc(k model.RebalanceTables) string {
	switch k.ShardingType.Type {
	case Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Decimal, DateTime:
		return fmt.Sprintf("CAST(`%s`, 'UInt64')", k.ShardingKey)
	case String:
		return fmt.Sprintf("xxHash64(`%s`)", k.ShardingKey)
	}
	return ""
}
