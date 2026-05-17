package rebalance

import (
	"math"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestComputeImbalance(t *testing.T) {
	cases := []struct {
		name   string
		shards []model.ShardRebalanceInfo
		want   float64
	}{
		{
			name:   "empty",
			shards: nil,
			want:   0,
		},
		{
			name: "balanced",
			shards: []model.ShardRebalanceInfo{
				{Rows: 100},
				{Rows: 100},
				{Rows: 100},
				{Rows: 100},
			},
			want: 0,
		},
		{
			name: "all zero",
			shards: []model.ShardRebalanceInfo{
				{Rows: 0},
				{Rows: 0},
			},
			want: 0,
		},
		{
			name: "single shard never imbalanced",
			shards: []model.ShardRebalanceInfo{
				{Rows: 12345},
			},
			want: 0,
		},
		{
			// avg=50, max=100, (100-50)/50 = 1.0
			name: "100% imbalance",
			shards: []model.ShardRebalanceInfo{
				{Rows: 100},
				{Rows: 50},
				{Rows: 50},
				{Rows: 0},
			},
			want: 1.0,
		},
		{
			// avg=85, max=100, (100-85)/85 ≈ 0.1765
			name: "mild imbalance",
			shards: []model.ShardRebalanceInfo{
				{Rows: 100},
				{Rows: 90},
				{Rows: 80},
				{Rows: 70},
			},
			want: 15.0 / 85.0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := computeImbalance(c.shards)
			if math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("computeImbalance(%v) = %v, want %v", c.shards, got, c.want)
			}
		})
	}
}

func TestCollectWarnings(t *testing.T) {
	t.Run("under-partitioned shard triggers warning with ZH+EN text", func(t *testing.T) {
		info := model.TableRebalanceInfo{
			Shards: []model.ShardRebalanceInfo{
				{PartitionCount: 5},
				{PartitionCount: 1}, // the offender
				{PartitionCount: 10},
			},
		}
		warns := collectWarnings(info)
		if len(warns) != 1 {
			t.Fatalf("expected 1 warning, got %d: %v", len(warns), warns)
		}
		if warns[0].ZH == "" || warns[0].EN == "" {
			t.Fatalf("warning should carry both ZH and EN: %+v", warns[0])
		}
	})

	t.Run("zero-partition shard also triggers", func(t *testing.T) {
		info := model.TableRebalanceInfo{
			Shards: []model.ShardRebalanceInfo{
				{PartitionCount: 0},
				{PartitionCount: 5},
			},
		}
		warns := collectWarnings(info)
		if len(warns) != 1 {
			t.Fatalf("expected 1 warning for 0-partition shard, got %d", len(warns))
		}
	})

	t.Run("well-partitioned silent", func(t *testing.T) {
		info := model.TableRebalanceInfo{
			Shards: []model.ShardRebalanceInfo{
				{PartitionCount: 100},
				{PartitionCount: 200},
			},
		}
		warns := collectWarnings(info)
		if len(warns) != 0 {
			t.Fatalf("expected no warnings, got %v", warns)
		}
	})

	t.Run("empty shards warns by treating minParts as 0", func(t *testing.T) {
		// minParts starts at 0 and stays 0 when there are no shards to
		// inspect, so the <=1 branch fires. Intentional: an empty shard
		// list is a suspect state worth flagging in the UI.
		info := model.TableRebalanceInfo{}
		warns := collectWarnings(info)
		if len(warns) != 1 {
			t.Fatalf("expected 1 warning for empty shards, got %v", warns)
		}
	})
}
