package rebalance

import (
	"fmt"
	"strings"
	"sync"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

// InitCKConns opens pooled ClickHouse connections to every host in Hosts and
// allocates the per-host serialization mutex used by partition fetch/attach.
// When withShardingkey is true it additionally fetches the engine signature,
// sorting key (for Replacing engines) and current row count, which are
// consumed by the shardingkey strategy's count verification.
func (r *Rebalancer) InitCKConns(withShardingkey bool) error {
	r.locks = make(map[string]*sync.Mutex)
	for _, host := range r.Hosts {
		if _, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, r.ConnOpt); err != nil {
			return err
		}
		log.Logger.Infof("[rebalance]initialized clickhouse connection to %s", host)
		r.locks[host] = &sync.Mutex{}
	}
	if !withShardingkey {
		return nil
	}
	return r.fetchTableMetadata()
}

// fetchTableMetadata pulls engine_full / sorting key / original row count from
// the first host so that the shardingkey path can build the tmp table with the
// same engine clause and verify nothing was lost in the round trip.
//
// NOTE (refactor): the pre-refactor code used `rows, _ := conn.Query(...)`
// for the sortingkey and count subqueries, silently discarding their errors.
// During the package extraction those calls were tightened to propagate the
// error rather than continuing with garbage state — strictly speaking a
// behavior change from "ignore and proceed" to "fail fast", but in practice
// the original code would have failed downstream on the empty result anyway.
func (r *Rebalancer) fetchTableMetadata() error {
	conn := common.GetConnection(r.Hosts[0])
	if conn == nil {
		return fmt.Errorf("[rebalance]can't get connection: %s", r.Hosts[0])
	}
	query := fmt.Sprintf("SELECT engine, engine_full FROM system.tables WHERE database = '%s' AND table = '%s'", r.Database, r.Table)
	log.Logger.Debugf("[rebalance]query:%s", query)
	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		if err := rows.Scan(&r.Engine, &r.EngineFull); err != nil {
			rows.Close()
			return err
		}
	}
	rows.Close()
	log.Logger.Infof("[rebalance]table: %s.%s, engine: %s, engine_full:%s", r.Database, r.Table, r.Engine, r.EngineFull)

	if strings.Contains(r.Engine, "Replacing") {
		query = fmt.Sprintf("SELECT name FROM system.columns WHERE (database = '%s') AND (table = '%s') AND (is_in_sorting_key = 1)", r.Database, r.Table)
		log.Logger.Debugf("[rebalance]query:%s", query)
		rows, err := conn.Query(query)
		if err != nil {
			return err
		}
		for rows.Next() {
			var sortingkey string
			if err := rows.Scan(&sortingkey); err != nil {
				rows.Close()
				return err
			}
			r.SortingKey = append(r.SortingKey, sortingkey)
		}
		rows.Close()
		log.Logger.Infof("[rebalance]table: %s.%s, sortingkey:%s", r.Database, r.Table, r.SortingKey)
	}

	var countQuery string
	if strings.Contains(r.Engine, "Replacing") {
		countQuery = fmt.Sprintf("SELECT count() FROM (SELECT DISTINCT %s FROM cluster('%s', '%s.%s') FINAL)", strings.Join(r.SortingKey, ","), r.Cluster, r.Database, r.Table)
	} else {
		countQuery = fmt.Sprintf("SELECT count() FROM cluster('%s', '%s.%s')", r.Cluster, r.Database, r.Table)
	}
	log.Logger.Debugf("query: %s", countQuery)
	rows, err = conn.Query(countQuery)
	if err != nil {
		return err
	}
	var oriCount uint64
	for rows.Next() {
		if err := rows.Scan(&oriCount); err != nil {
			rows.Close()
			return err
		}
	}
	rows.Close()
	r.OriCount = uint64((1 - r.AllowLossRate) * float64(oriCount))
	log.Logger.Infof("table: %s.%s, oriCount: %d, count: %d", r.Database, r.Table, oriCount, r.OriCount)
	return nil
}
