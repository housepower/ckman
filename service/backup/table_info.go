package backup

import (
	"fmt"
	"sort"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
)

// TablePartitionInfo 描述一张表的分区方式与大小。
type TablePartitionInfo struct {
	Name            string `json:"name"`
	PartitionKey    string `json:"partition_key"`
	PartitionFormat string `json:"partition_format"` // day | month | hour | custom | none
	DailyCompatible bool   `json:"daily_compatible"`
	TotalBytes      uint64 `json:"total_bytes"`
}

// PartitionDetail 是一个分区的名称与体量，供前端「按分区备份」时勾选。
type PartitionDetail struct {
	Partition string `json:"partition"`
	Bytes     uint64 `json:"bytes"`
	Rows      uint64 `json:"rows"`
}

// ListTablePartitions 列 cluster.database.table 的全部 active 分区及体量，
// 按分区名倒序（最近的在前），上限 10000 条。供备份表单的分区选择器用。
//
// 体量是集群真实总量：每个 shard 各查第一个可达副本的本地 system.parts
// 再按分区累加——不能用 clusterAllReplicas 一次性 sum，那会把每个副本的
// 重复拷贝都计入（体积虚高副本数倍）。
func (s *Service) ListTablePartitions(cluster, database, table string) ([]PartitionDetail, error) {
	ch := GetChAdapter()
	if ch == nil {
		return nil, fmt.Errorf("clickhouse adapter not initialized")
	}
	for _, ident := range []string{database, table} {
		if err := ValidateIdentifier(ident); err != nil {
			return nil, fmt.Errorf("invalid identifier %q: %w", ident, err)
		}
	}
	cc, err := ch.getCluster(cluster)
	if err != nil {
		return nil, err
	}
	if len(cc.Shards) == 0 {
		return nil, fmt.Errorf("cluster %s has no shards", cluster)
	}

	// conn 走公共连接池（GetConnOption 默认 BypassPool=false），不能 Close，
	// 否则池里会留下一条死连接（同 GetTablePartitionSummary 的处理）。
	agg := map[string]*PartitionDetail{}
	for idx, shard := range cc.Shards {
		var conn *common.Conn
		for _, replica := range shard.Replicas {
			c, derr := ch.dial(replica.Ip, cc.GetConnOption())
			if derr != nil {
				log.Logger.Warnf("[backup] ListTablePartitions shard %d dial %s: %v", idx, replica.Ip, derr)
				continue
			}
			conn = c
			break
		}
		if conn == nil {
			return nil, fmt.Errorf("no reachable replica in shard %d of cluster %s", idx, cluster)
		}
		// 本地 system.parts 只含本 shard 的数据，按分区聚合后与 agg 累加
		query := fmt.Sprintf(
			"SELECT partition, sum(bytes_on_disk), sum(rows) FROM system.parts WHERE database = '%s' AND `table` = '%s' AND active = 1 GROUP BY partition",
			strings.ReplaceAll(database, "'", "''"),
			strings.ReplaceAll(table, "'", "''"),
		)
		rows, err := conn.Query(query)
		if err != nil {
			return nil, fmt.Errorf("query partitions on shard %d: %w", idx, err)
		}
		for rows.Next() {
			var p string
			var b, r uint64
			if err := rows.Scan(&p, &b, &r); err != nil {
				rows.Close()
				return nil, err
			}
			if d, ok := agg[p]; ok {
				d.Bytes += b
				d.Rows += r
			} else {
				agg[p] = &PartitionDetail{Partition: p, Bytes: b, Rows: r}
			}
		}
		rows.Close()
	}

	out := make([]PartitionDetail, 0, len(agg))
	for _, d := range agg {
		out = append(out, *d)
	}
	// 分区名倒序：按天/月分区即最近在前，方便用户勾选
	sort.Slice(out, func(i, j int) bool { return out[i].Partition > out[j].Partition })
	if len(out) > 10000 {
		out = out[:10000]
	}
	return out, nil
}

// GetTablePartitionSummary 查询 cluster.database 下所有 MergeTree 系列表的
// partition_key 与 total_bytes。前端选表时 batch 调用并本地缓存几分钟。
//
// bytes 查询失败不影响主返回（size 是辅助信息）。
func (s *Service) GetTablePartitionSummary(cluster, database string) ([]TablePartitionInfo, error) {
	ch := GetChAdapter()
	if ch == nil {
		return nil, fmt.Errorf("clickhouse adapter not initialized")
	}

	cc, err := ch.getCluster(cluster)
	if err != nil {
		return nil, err
	}
	if len(cc.Shards) == 0 || len(cc.Shards[0].Replicas) == 0 {
		return nil, fmt.Errorf("cluster %s has no replicas", cluster)
	}
	host := cc.Shards[0].Replicas[0].Ip
	conn, err := ch.dial(host, cc.GetConnOption())
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", host, err)
	}

	// 1. 查 system.tables 拿 name + partition_key
	dbEsc := strings.ReplaceAll(database, "'", "''")
	q1 := fmt.Sprintf(
		"SELECT name, partition_key FROM system.tables WHERE database = '%s' AND engine LIKE '%%MergeTree%%' ORDER BY name",
		dbEsc,
	)
	rows1, err := conn.Query(q1)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	defer rows1.Close()

	infos := []TablePartitionInfo{}
	byName := map[string]*TablePartitionInfo{}
	for rows1.Next() {
		var name, pkey string
		if err := rows1.Scan(&name, &pkey); err != nil {
			return nil, err
		}
		infos = append(infos, TablePartitionInfo{
			Name:            name,
			PartitionKey:    pkey,
			PartitionFormat: PartitionFmt(pkey),
			DailyCompatible: IsDailyCompatible(pkey),
		})
		byName[name] = &infos[len(infos)-1]
	}

	// 2. 查 system.parts 聚合 total_bytes（跨节点）；失败不影响主返回
	q2 := fmt.Sprintf(
		"SELECT table, sum(bytes_on_disk) AS total FROM cluster('default', system.parts) WHERE database = '%s' AND active = 1 GROUP BY table",
		dbEsc,
	)
	rows2, err := conn.Query(q2)
	if err != nil {
		return infos, nil
	}
	defer rows2.Close()
	for rows2.Next() {
		var tbl string
		var total uint64
		if err := rows2.Scan(&tbl, &total); err != nil {
			continue
		}
		if info, ok := byName[tbl]; ok {
			info.TotalBytes = total
		}
	}
	return infos, nil
}
