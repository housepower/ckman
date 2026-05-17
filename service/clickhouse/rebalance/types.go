// Package rebalance implements ClickHouse cluster data rebalancing across shards.
//
// Two strategies are supported, selected per table:
//
//   - partition:   moves whole partitions between hosts (cheap, metadata-level on
//                  replicated clusters via FETCH PARTITION; rsync-based on
//                  non-replicated clusters).
//   - shardingkey: re-hashes every row by a user-supplied column, used when the
//                  data was originally inserted without going through the
//                  Distributed engine's own sharding expression.
//
// Top-level entry points are Run (execute a rebalance) and Info (inspect the
// current row/byte distribution). Both are wrapped by service/clickhouse for
// backwards-compatible package paths.
package rebalance

import (
	"sync"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

// ClickHouse column type discriminators consumed by ShardingFunc and WhichType.
// Kept here (rather than in model) because they are only meaningful in the
// rebalance context's sharding hash selection.
const (
	Unknown = iota
	Bool
	Int8
	Int16
	Int32
	Int64
	UInt8
	UInt16
	UInt32
	UInt64
	Float32
	Float64
	Decimal
	DateTime
	String
)

// Config carries everything statically known before a rebalance starts. It is
// the input contract: callers (the runner handle or tests) fill this in and
// hand a *Rebalancer to a Strategy.
type Config struct {
	Cluster       string
	Hosts         []string
	DataDir       string
	Database      string
	Table         string
	TmpTable      string
	DistTable     string
	IsReplica     bool
	OsUser        string
	OsPassword    string
	OsPort        int
	Shardingkey   model.RebalanceTables
	ExceptHost    string
	ConnOpt       model.ConnetOption
	AllowLossRate float64
	SaveTemps     bool
}

// Rebalancer is the runtime container threaded through a Strategy. It owns
// per-run state (connections, locks, discovered table metadata) so that
// concurrent rebalances on different clusters do not share mutable state.
type Rebalancer struct {
	Config

	// Discovered table metadata, populated during InitCKConns.
	RepTables  map[string]string
	Engine     string
	EngineFull string
	OriCount   uint64
	SortingKey []string

	// Concurrency control scoped to this rebalance. A prior bug had these as
	// package vars; cross-cluster runs could corrupt each other.
	sshErr error
	locks  map[string]*sync.Mutex
}

// New constructs a Rebalancer from Config with empty runtime state.
func New(cfg Config) *Rebalancer {
	return &Rebalancer{
		Config:    cfg,
		RepTables: make(map[string]string),
	}
}

// Close releases any pooled ClickHouse connections opened for this run.
func (r *Rebalancer) Close() {
	common.CloseConns(r.Hosts)
}
