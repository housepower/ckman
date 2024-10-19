package clickhouse

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
)

var (
	sshErr error
	locks  map[string]*sync.Mutex
)

type CKRebalance struct {
	Cluster       string
	Hosts         []string
	DataDir       string
	Database      string
	Table         string
	TmpTable      string
	DistTable     string
	IsReplica     bool
	RepTables     map[string]string
	OsUser        string
	OsPassword    string
	OsPort        int
	Shardingkey   model.RebalanceShardingkey
	ExceptHost    string
	ConnOpt       model.ConnetOption
	Engine        string
	EngineFull    string
	OriCount      uint64
	SortingKey    []string
	AllowLossRate float64
	SaveTemps     bool
}

// TblPartitions is partitions status of a host. A host never move out and move in at the same iteration.
type TblPartitions struct {
	Table      string
	Host       string
	ZooPath    string // zoo-path with macros substituted
	Partitions map[string]uint64
	TotalSize  uint64            // total size of partitions
	ToMoveOut  map[string]string // plan to move some partitions out to other hosts
	ToMoveIn   bool              // plan to move some partitions in
}

func (r *CKRebalance) InitCKConns(withShardingkey bool) (err error) {
	locks = make(map[string]*sync.Mutex)
	for _, host := range r.Hosts {
		_, err = common.ConnectClickHouse(host, model.ClickHouseDefaultDB, r.ConnOpt)
		if err != nil {
			return
		}
		log.Logger.Infof("[rebalance]initialized clickhouse connection to %s", host)
		locks[host] = &sync.Mutex{}
	}

	if withShardingkey {
		conn := common.GetConnection(r.Hosts[0])
		// get engine
		query := fmt.Sprintf("SELECT engine, engine_full FROM system.tables WHERE database = '%s' AND table = '%s'", r.Database, r.Table)
		log.Logger.Debugf("[rebalance]query:%s", query)
		rows, _ := conn.Query(query)
		for rows.Next() {
			err = rows.Scan(&r.Engine, &r.EngineFull)
			if err != nil {
				return
			}
		}
		rows.Close()
		log.Logger.Infof("[rebalance]table: %s.%s, engine: %s, engine_full:%s", r.Database, r.Table, r.Engine, r.EngineFull)

		//get sortingkey
		if strings.Contains(r.Engine, "Replacing") {
			query = fmt.Sprintf("SELECT name FROM system.columns WHERE (database = '%s') AND (table = '%s') AND (is_in_sorting_key = 1)", r.Database, r.Table)
			log.Logger.Debugf("[rebalance]query:%s", query)
			rows, _ := conn.Query(query)
			for rows.Next() {
				var sortingkey string
				err = rows.Scan(&sortingkey)
				if err != nil {
					return
				}
				r.SortingKey = append(r.SortingKey, sortingkey)
			}
			rows.Close()
			log.Logger.Infof("[rebalance]table: %s.%s, sortingkey:%s", r.Database, r.Table, r.SortingKey)

		}

		//get original count
		if strings.Contains(r.Engine, "Replacing") {
			query = fmt.Sprintf("SELECT count() FROM (SELECT DISTINCT %s FROM cluster('%s', '%s.%s') FINAL)", strings.Join(r.SortingKey, ","), r.Cluster, r.Database, r.Table)
		} else {
			query = fmt.Sprintf("SELECT count() FROM cluster('%s', '%s.%s')", r.Cluster, r.Database, r.Table)
		}
		log.Logger.Debugf("query: %s", query)
		rows, _ = conn.Query(query)
		var oriCount uint64
		for rows.Next() {
			err = rows.Scan(&oriCount)
			if err != nil {
				return
			}
		}
		r.OriCount = uint64((1 - r.AllowLossRate) * float64(oriCount))
		log.Logger.Infof("table: %s.%s, oriCount: %d, count: %d", r.Database, r.Table, oriCount, r.OriCount)
		rows.Close()
	}
	return
}

func (r *CKRebalance) GetRepTables() (err error) {
	if !r.IsReplica {
		return nil
	}
	for _, host := range r.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			return fmt.Errorf("[rebalance]can't get connection: %s", host)
		}
		query := fmt.Sprintf("SELECT zookeeper_path FROM system.replicas WHERE database='%s' AND table = '%s'", r.Database, r.Table)
		log.Logger.Infof("[rebalance]host %s: query: %s", host, query)
		var rows *common.Rows
		if rows, err = conn.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		var zookeeper_path string
		for rows.Next() {
			_ = rows.Scan(&zookeeper_path)
			if _, ok := r.RepTables[host]; !ok {
				r.RepTables[host] = zookeeper_path
			}
		}
	}
	return
}

func (r *CKRebalance) InitSshConns() (err error) {
	// Validate if one can login from any host to another host without password, and read the data directory.
	for _, srcHost := range r.Hosts {
		for _, dstHost := range r.Hosts {
			if srcHost == dstHost {
				continue
			}
			cmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=false %s sudo ls %sclickhouse/data/%s", dstHost, r.DataDir, r.Database)
			log.Logger.Infof("[rebalance]host: %s, command: %s", srcHost, cmd)
			sshOpts := common.SshOptions{
				User:             r.OsUser,
				Password:         r.OsPassword,
				Port:             r.OsPort,
				Host:             srcHost,
				NeedSudo:         false,
				AuthenticateType: model.SshPasswordSave,
			}
			var out string
			if out, err = common.RemoteExecute(sshOpts, cmd); err != nil {
				return
			}
			log.Logger.Debugf("[rebalance]host: %s, output: %s", srcHost, out)
		}
	}
	return
}

func (r *CKRebalance) GetPartState() (tbls []*TblPartitions, err error) {
	tbls = make([]*TblPartitions, 0)
	for _, host := range r.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			err = fmt.Errorf("[rebalance]can't get connection: %s", host)
			return
		}
		var rows *common.Rows
		// Skip the newest partition on each host since into which there could by ongoing insertions.
		query := fmt.Sprintf(`WITH (SELECT argMax(partition, modification_time) FROM system.parts WHERE database='%s' AND table='%s') AS latest_partition SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1 AND partition!=latest_partition GROUP BY partition ORDER BY partition;`, r.Database, r.Table, r.Database, r.Table)
		log.Logger.Infof("[rebalance]host %s: query: %s", host, query)
		if rows, err = conn.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		tbl := TblPartitions{
			Table:      fmt.Sprintf("%s.%s", r.Database, r.Table),
			Host:       host,
			Partitions: make(map[string]uint64),
		}
		for rows.Next() {
			var patt string
			var compressed uint64
			if err = rows.Scan(&patt, &compressed); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			tbl.Partitions[patt] = compressed
			tbl.TotalSize += compressed
		}
		if zoopath, ok := r.RepTables[host]; ok {
			tbl.ZooPath = zoopath
		}
		tbls = append(tbls, &tbl)
	}
	log.Logger.Infof("[rebalance]table %s state %s", r.Table, pp.Sprint(tbls))
	return
}

func (r *CKRebalance) GeneratePartPlan(tbls []*TblPartitions) {
	for {
		sort.Slice(tbls, func(i, j int) bool { return tbls[i].TotalSize < tbls[j].TotalSize })
		numTbls := len(tbls)
		var minIdx, maxIdx int
		for minIdx = 0; minIdx < numTbls && tbls[minIdx].ToMoveOut != nil; minIdx++ {
		}
		for maxIdx = numTbls - 1; maxIdx >= 0 && tbls[maxIdx].ToMoveIn; maxIdx-- {
		}
		if minIdx >= maxIdx {
			break
		}
		minTbl := tbls[minIdx]
		maxTbl := tbls[maxIdx]
		var found bool
		for patt, pattSize := range maxTbl.Partitions {
			if maxTbl.TotalSize >= minTbl.TotalSize+2*pattSize {
				minTbl.TotalSize += pattSize
				minTbl.ToMoveIn = true
				maxTbl.TotalSize -= pattSize
				if maxTbl.ToMoveOut == nil {
					maxTbl.ToMoveOut = make(map[string]string)
				}
				maxTbl.ToMoveOut[patt] = minTbl.Host
				delete(maxTbl.Partitions, patt)
				found = true
				break
			}
		}
		if !found {
			for _, tbl := range tbls {
				tbl.Partitions = nil
			}
			break
		}
	}
	for _, tbl := range tbls {
		tbl.Partitions = nil
	}
}

func (r *CKRebalance) ExecutePartPlan(tbl *TblPartitions) (err error) {
	if tbl.ToMoveOut == nil {
		return
	}
	if tbl.ZooPath != "" {
		// 带副本集群
		for patt, dstHost := range tbl.ToMoveOut {
			lock := locks[dstHost]

			// There could be multiple executions on the same dest node and partition.
			lock.Lock()
			dstChConn := common.GetConnection(dstHost)
			if dstChConn == nil {
				return fmt.Errorf("[rebalance]can't get connection: %s", dstHost)
			}
			dstQuires := []string{
				fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s' ", tbl.Table, patt),
				fmt.Sprintf("ALTER TABLE %s FETCH PARTITION '%s' FROM '%s'", tbl.Table, patt, tbl.ZooPath),
				fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt),
			}
			for _, query := range dstQuires {
				log.Logger.Infof("[rebalance]host %s: query: %s", dstHost, query)
				if err = dstChConn.Exec(query); err != nil {
					err = errors.Wrapf(err, "")
					return
				}
			}
			lock.Unlock()

			srcChConn := common.GetConnection(tbl.Host)
			if srcChConn == nil {
				return fmt.Errorf("[rebalance]can't get connection: %s", tbl.Host)
			}
			query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tbl.Table, patt)
			if err = srcChConn.Exec(query); err != nil {
				log.Logger.Infof("[rebalance]host %s: query: %s", tbl.Host, query)
				err = errors.Wrapf(err, "")
				return
			}
		}
		return
	} else {
		//不带副本集群， 公钥认证集群做不了
		if sshErr != nil {
			log.Logger.Warnf("[rebalance]skip execution for %s due to previous SSH error", tbl.Table)
			return
		}
		for patt, dstHost := range tbl.ToMoveOut {
			srcCkConn := common.GetConnection(tbl.Host)
			dstCkConn := common.GetConnection(dstHost)
			if srcCkConn == nil || dstCkConn == nil {
				log.Logger.Errorf("[rebalance]can't get connection: %s & %s", tbl.Host, dstHost)
				return
			}
			lock := locks[dstHost]
			tableName := strings.Split(tbl.Table, ".")[1]
			dstDir := filepath.Join(r.DataDir, fmt.Sprintf("clickhouse/data/%s/%s/detached", r.Database, tableName))
			srcDir := dstDir + "/"

			query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", tbl.Table, patt)
			log.Logger.Infof("[rebalance]host: %s, query: %s", tbl.Host, query)
			if err = srcCkConn.Exec(query); err != nil {
				err = errors.Wrapf(err, "")
				return
			}

			// There could be multiple executions on the same dest node and partition.
			lock.Lock()
			cmds := []string{
				fmt.Sprintf(`rsync -e "ssh -o StrictHostKeyChecking=false" -avp %s %s:%s`, srcDir, dstHost, dstDir),
				fmt.Sprintf("rm -fr %s", srcDir),
			}
			sshOpts := common.SshOptions{
				User:             r.OsUser,
				Password:         r.OsPassword,
				Port:             r.OsPort,
				Host:             tbl.Host,
				NeedSudo:         true,
				AuthenticateType: model.SshPasswordSave,
			}
			var out string
			if out, err = common.RemoteExecute(sshOpts, strings.Join(cmds, ";")); err != nil {
				lock.Unlock()
				return
			}
			log.Logger.Debugf("[rebalance]host: %s, output: %s", tbl.Host, out)

			query = fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt)
			log.Logger.Infof("[rebalance]host: %s, query: %s", dstHost, query)
			if err = dstCkConn.Exec(query); err != nil {
				err = errors.Wrapf(err, "")
				lock.Unlock()
				return
			}
			lock.Unlock()

			query = fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s'", tbl.Table, patt)
			log.Logger.Infof("[rebalance]host: %s, query: %s", tbl.Host, query)
			if err = srcCkConn.Exec(query); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
	}
	return
}

func (r *CKRebalance) DoRebalanceByPart() (err error) {

	// initialize SSH connections only if there are some non-replicated tables
	if !r.IsReplica {
		if sshErr = r.InitSshConns(); sshErr != nil {
			log.Logger.Warnf("[rebalance]failed to init ssh connections, error: %+v", sshErr)
		}
	}
	var tbls []*TblPartitions
	if tbls, err = r.GetPartState(); err != nil {
		log.Logger.Errorf("[rebalance]got error %+v", err)
		return err
	}
	r.GeneratePartPlan(tbls)

	var gotError bool
	var wg sync.WaitGroup
	for i := 0; i < len(tbls); i++ {
		tbl := tbls[i]
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			if err := r.ExecutePartPlan(tbl); err != nil {
				log.Logger.Errorf("[rebalance]host: %s, got error %+v", tbl.Host, err)
				gotError = true
			} else {
				log.Logger.Infof("[rebalance]table %s host %s rebalance done", tbl.Table, tbl.Host)
			}
		})
	}
	wg.Wait()
	if gotError {
		return err
	}
	log.Logger.Infof("[rebalance]table %s.%s rebalance done", r.Database, r.Table)
	return
}

func (r *CKRebalance) Close() {
	common.CloseConns(r.Hosts)
}

func (r *CKRebalance) Cleanup() {
	conn := common.GetConnection(r.Hosts[0])
	cleanSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", r.Database, r.TmpTable, r.Cluster)
	log.Logger.Debugf("[%s]%s", r.Hosts[0], cleanSql)
	if err := conn.Exec(cleanSql); err != nil {
		log.Logger.Warnf("drop table %s.%s failed: %v", r.Database, r.TmpTable, err)
	}
}

func (r *CKRebalance) CreateTemporaryTable() error {
	// if ckman crashed when rebalancing, cleanup tmp table to ensure next rebalance successfully
	r.Cleanup()
	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE=%s",
		r.Database, r.TmpTable, r.Cluster, r.Database, r.Table, r.EngineFull)
	create = strings.ReplaceAll(create, fmt.Sprintf("/%s/", r.Table), fmt.Sprintf("/%s/", r.TmpTable)) // replace engine zoopath
	log.Logger.Debug(create)
	if create != "" {
		conn := common.GetConnection(r.Hosts[0])
		err := conn.Exec(create)
		if err != nil {
			return errors.Wrap(err, r.Hosts[0])
		}
	}
	return nil
}

func (r *CKRebalance) CheckCounts(tableName string) error {
	var query string
	if strings.Contains(r.Engine, "Replacing") {
		query = fmt.Sprintf("SELECT count() FROM (SELECT DISTINCT %s FROM cluster('%s', '%s.%s') FINAL)", strings.Join(r.SortingKey, ","), r.Cluster, r.Database, tableName)
	} else {
		query = fmt.Sprintf("SELECT count() FROM cluster('%s', '%s.%s')", r.Cluster, r.Database, tableName)
	}
	log.Logger.Debugf("query: %s", query)
	conn := common.GetConnection(r.Hosts[0])
	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	var count uint64
	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return err
		}
	}
	log.Logger.Infof("table: %s.%s, count: %d", r.Database, tableName, count)
	rows.Close()
	// 有可能在负载均衡期间数据还在往原始表写，所以最终数据应该是大于等于最原始统计的条数的，虽然我们并不建议这样做
	if count < r.OriCount {
		return fmt.Errorf("table %s count %d is less than original: %d", tableName, count, r.OriCount)
	}
	return nil
}

// moveback from tmp_table to ori_table after rehash
func (r *CKRebalance) InsertPlan() error {
	var lastError error
	var wg sync.WaitGroup
	for idx, host := range r.Hosts {
		idx := idx
		host := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			conn := common.GetConnection(host)
			query := fmt.Sprintf(`SELECT distinct partition_id FROM cluster('%s', 'system.parts') WHERE database = '%s' AND table = '%s' AND active=1 ORDER BY partition_id DESC`, r.Cluster, r.Database, r.TmpTable)
			log.Logger.Debugf("[%s]%s", host, query)
			rows, err := conn.Query(query)
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}
			partitions := make([]string, 0)
			for rows.Next() {
				var partitionId string
				err = rows.Scan(&partitionId)
				if err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
				partitions = append(partitions, partitionId)
			}
			rows.Close()
			log.Logger.Debugf("host:[%s], parts: %v", host, partitions)

			for i, partition := range partitions {
				query = fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM cluster('%s', '%s.%s') WHERE _partition_id = '%s' AND %s %% %d = %d SETTINGS insert_deduplicate=false,max_execution_time=0,max_insert_threads=8",
					r.Database, r.Table, r.Cluster, r.Database, r.TmpTable, partition, ShardingFunc(r.Shardingkey), len(r.Hosts), idx)
				log.Logger.Debugf("[%s](%d/%d) %s", host, i+1, len(partitions), query)
				if err = conn.Exec(query); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
			}
		})
	}
	wg.Wait()
	return lastError
}

// backup from ori_table to tmp_table
func (r *CKRebalance) MoveBackup() error {
	var wg sync.WaitGroup
	var lastError error
	for _, host := range r.Hosts {
		host := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			conn := common.GetConnection(host)
			query := fmt.Sprintf(`SELECT distinct partition_id FROM system.parts WHERE database = '%s' AND table = '%s' AND active=1 order by partition_id`, r.Database, r.Table)
			log.Logger.Debugf("[%s]%s", host, query)
			rows, err := conn.Query(query)
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}
			partitions := make([]string, 0)
			for rows.Next() {
				var partitionId string
				err = rows.Scan(&partitionId)
				if err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
				partitions = append(partitions, partitionId)
			}
			rows.Close()
			log.Logger.Debugf("host:[%s], partitions: %v", host, partitions)

			for idx, partition := range partitions {
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` MOVE PARTITION ID '%s' TO TABLE `%s`.`%s`", r.Database, r.Table, partition, r.Database, r.TmpTable)
				log.Logger.Debugf("[%s](%d/%d) %s", host, idx+1, len(partitions), query)
				if err = conn.Exec(query); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
			}
		})
	}
	wg.Wait()
	return lastError
}
