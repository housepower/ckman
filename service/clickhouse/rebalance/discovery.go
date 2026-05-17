package rebalance

import (
	"fmt"
	"regexp"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

// BalanceableTable describes one rebalanceable table discovered by scanning the
// cluster for Distributed engines. The matching local table on each shard
// shares this distributed table's schema and is the actual rebalance target.
type BalanceableTable struct {
	Database  string
	DistTable string
	Table     string   // the local MergeTree table name behind the Distributed
	Columns   []string // candidate columns suitable as sharding keys
}

// listBalanceableTables returns every (Distributed → local) table pair in the
// cluster whose local table has at least one numeric/date/string column. The
// query is intentionally permissive — paddingKeys then narrows by the user's
// regex on table name and validates the requested sharding key.
func listBalanceableTables(conn *common.Conn, cluster string) ([]BalanceableTable, error) {
	query := fmt.Sprintf(`SELECT
    t2.database AS database,
	t2.name AS dist,
    t2.local AS table,
    groupArray(t1.name) AS rows
FROM system.columns AS t1
INNER JOIN
(
	SELECT
    database,
	name,
    (extractAllGroups(engine_full, '(Distributed\(\')(.*)\',\s+\'(.*)\',\s+\'(.*)\'(.*)')[1])[2] AS cluster,
    (extractAllGroups(engine_full, '(Distributed\(\')(.*)\',\s+\'(.*)\',\s+\'(.*)\'(.*)')[1])[4] AS local
FROM system.tables
WHERE match(engine, 'Distributed') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
AND cluster = '%s')
) AS t2 ON t1.table = t2.name and t1.database=t2.database
WHERE (multiSearchAny(t1.type, ['Int', 'Float', 'Date', 'String', 'Decimal']) = '1')
GROUP BY
    database,
	dist,
    table
ORDER BY
    database
`, cluster)

	log.Logger.Debug(query)
	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]BalanceableTable, 0)
	for rows.Next() {
		var database, dist, table string
		var cols []string
		if err := rows.Scan(&database, &dist, &table, &cols); err != nil {
			return nil, err
		}
		out = append(out, BalanceableTable{
			Database:  database,
			DistTable: dist,
			Table:     table,
			Columns:   cols,
		})
	}
	return out, nil
}

// paddingKeys expands user-supplied table patterns (regex on the table name)
// to the actual set of balanceable tables in the cluster, carrying the
// requested policy/shardingKey through. For shardingkey policy, the chosen
// column must exist on every matched table or an error is returned to fail
// fast before any data is moved.
func paddingKeys(conn *common.Conn, cluster string, rtables []model.RebalanceTables) ([]model.RebalanceTables, error) {
	var results []model.RebalanceTables
	resps, err := listBalanceableTables(conn, cluster)
	if err != nil {
		return rtables, err
	}
	for _, elem := range rtables {
		reg, err := regexp.Compile(elem.Table)
		if err != nil {
			return rtables, err
		}
		for _, rt := range resps {
			rtable := model.RebalanceTables{
				Database:    rt.Database,
				Table:       rt.Table,
				DistTable:   rt.DistTable,
				Policy:      elem.Policy,
				ShardingKey: elem.ShardingKey,
			}
			if rtable.Database != elem.Database || !reg.MatchString(rtable.Table) {
				continue
			}
			duplicated := false
			for _, r := range results {
				if r.Database == rtable.Database && r.Table == rtable.Table {
					duplicated = true
					break
				}
			}
			if duplicated {
				continue
			}
			if rtable.Policy == model.RebalancePolicyShardingKey {
				if !common.ArraySearch(elem.ShardingKey, rt.Columns) {
					return rtables, fmt.Errorf("shardingKey %s not found in table %s.%s", elem.ShardingKey, rtable.Database, rtable.Table)
				}
				rtable.ShardingKey = elem.ShardingKey
			}
			results = append(results, rtable)
		}
	}
	return results, nil
}

// getShardingType looks up the ClickHouse type of the chosen sharding column
// and sets ShardingType on key. Rebalance refuses Nullable and Array columns
// because the hash expressions can't safely handle them.
func getShardingType(key *model.RebalanceTables, conn *common.Conn) error {
	query := fmt.Sprintf("SELECT type FROM system.columns WHERE (database = '%s') AND (table = '%s') AND (name = '%s') ",
		key.Database, key.Table, key.ShardingKey)
	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			return err
		}
		key.ShardingType = WhichType(typ)
	}
	if key.ShardingType.Nullable || key.ShardingType.Array {
		return errors.Errorf("invalid shardingKey %s, expect its type be numerical or string", key.ShardingKey)
	}
	return nil
}
