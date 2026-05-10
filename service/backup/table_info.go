package backup

import (
	"fmt"
	"strings"
)

// TablePartitionInfo 描述一张表的分区方式与大小。
type TablePartitionInfo struct {
	Name            string `json:"name"`
	PartitionKey    string `json:"partition_key"`
	PartitionFormat string `json:"partition_format"` // day | month | hour | custom | none
	DailyCompatible bool   `json:"daily_compatible"`
	TotalBytes      uint64 `json:"total_bytes"`
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
