package clickhouse

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
)

var (
	sshErr error
	locks  map[string]*sync.Mutex
)

type CKRebalance struct {
	Cluster     string
	Hosts       []string
	Port        int
	User        string
	Password    string
	DataDir     string
	Database    string
	Table       string
	DistTable   string
	IsReplica   bool
	RepTables   map[string]string
	OsUser      string
	OsPassword  string
	OsPort      int
	Shardingkey model.RebalanceShardingkey
	ExceptHost  string
}

// TblPartitions is partitions status of a host. A host never move out and move in at the same iteration.
type TblPartitions struct {
	Table      string
	Host       string
	ZooPath    string // zoo-path with macros substituted
	Partitions map[string]int64
	TotalSize  int64             // total size of partitions
	ToMoveOut  map[string]string // plan to move some partitions out to other hosts
	ToMoveIn   bool              // plan to move some partitions in
}

func (r *CKRebalance) InitCKConns() (err error) {
	locks = make(map[string]*sync.Mutex)
	for _, host := range r.Hosts {
		_, err = common.ConnectClickHouse(host, r.Port, model.ClickHouseDefaultDB, r.User, r.Password)
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
		locks[host] = &sync.Mutex{}
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
			return fmt.Errorf("can't get connection: %s", host)
		}
		query := fmt.Sprintf("SELECT zookeeper_path FROM system.replicas WHERE database='%s' AND table = '%s'", r.Database, r.Table)
		log.Logger.Infof("host %s: query: %s", host, query)
		var rows driver.Rows
		if rows, err = conn.Query(context.Background(), query); err != nil {
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
			log.Logger.Infof("host: %s, command: %s", srcHost, cmd)
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
			log.Logger.Debugf("host: %s, output: %s", srcHost, out)
		}
	}
	return
}

func (r *CKRebalance) GetPartState() (tbls []*TblPartitions, err error) {
	tbls = make([]*TblPartitions, 0)
	for _, host := range r.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			err = fmt.Errorf("can't get connection: %s", host)
			return
		}
		var rows driver.Rows
		// Skip the newest partition on each host since into which there could by ongoing insertions.
		query := fmt.Sprintf(`WITH (SELECT argMax(partition, modification_time) FROM system.parts WHERE database='%s' AND table='%s') AS latest_partition SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1 AND partition!=latest_partition GROUP BY partition ORDER BY partition;`, r.Database, r.Table, r.Database, r.Table)
		log.Logger.Infof("host %s: query: %s", host, query)
		if rows, err = conn.Query(context.Background(), query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		tbl := TblPartitions{
			Table:      fmt.Sprintf("%s.%s", r.Database, r.Table),
			Host:       host,
			Partitions: make(map[string]int64),
		}
		for rows.Next() {
			var patt string
			var compressed int64
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
	log.Logger.Infof("table %s state %s", r.Table, pp.Sprint(tbls))
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
		for patt, dstHost := range tbl.ToMoveOut {
			lock := locks[dstHost]

			// There could be multiple executions on the same dest node and partition.
			lock.Lock()
			dstChConn := common.GetConnection(dstHost)
			if dstChConn == nil {
				return fmt.Errorf("can't get connection: %s", dstHost)
			}
			dstQuires := []string{
				fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s' ", tbl.Table, patt),
				fmt.Sprintf("ALTER TABLE %s FETCH PARTITION '%s' FROM '%s'", tbl.Table, patt, tbl.ZooPath),
				fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt),
			}
			for _, query := range dstQuires {
				log.Logger.Infof("host %s: query: %s", dstHost, query)
				if err = dstChConn.Exec(context.Background(), query); err != nil {
					err = errors.Wrapf(err, "")
					return
				}
			}
			lock.Unlock()

			srcChConn := common.GetConnection(tbl.Host)
			if srcChConn == nil {
				return fmt.Errorf("can't get connection: %s", tbl.Host)
			}
			query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tbl.Table, patt)
			if err = srcChConn.Exec(context.Background(), query); err != nil {
				log.Logger.Infof("host %s: query: %s", tbl.Host, query)
				err = errors.Wrapf(err, "")
				return
			}
		}
		return
	}
	if sshErr != nil {
		log.Logger.Warnf("skip execution for %s due to previous SSH error", tbl.Table)
		return
	}
	for patt, dstHost := range tbl.ToMoveOut {
		srcCkConn := common.GetConnection(tbl.Host)
		dstCkConn := common.GetConnection(dstHost)
		if srcCkConn == nil || dstCkConn == nil {
			log.Logger.Errorf("can't get connection: %s & %s", tbl.Host, dstHost)
			return
		}
		lock := locks[dstHost]
		tableName := strings.Split(tbl.Table, ".")[1]
		dstDir := filepath.Join(r.DataDir, fmt.Sprintf("clickhouse/data/%s/%s/detached", r.Database, tableName))
		srcDir := dstDir + "/"

		query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("host: %s, query: %s", tbl.Host, query)
		if err = srcCkConn.Exec(context.Background(), query); err != nil {
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
		log.Logger.Debugf("host: %s, output: %s", tbl.Host, out)

		query = fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("host: %s, query: %s", dstHost, query)
		if err = dstCkConn.Exec(context.Background(), query); err != nil {
			err = errors.Wrapf(err, "")
			lock.Unlock()
			return
		}
		lock.Unlock()

		query = fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("host: %s, query: %s", tbl.Host, query)
		if err = srcCkConn.Exec(context.Background(), query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}

func (r *CKRebalance) DoRebalanceByPart() (err error) {

	// initialize SSH connections only if there are some non-replicated tables
	if sshErr = r.InitSshConns(); sshErr != nil {
		log.Logger.Warnf("failed to init ssh connections, error: %+v", sshErr)
	}
	var tbls []*TblPartitions
	if tbls, err = r.GetPartState(); err != nil {
		log.Logger.Errorf("got error %+v", err)
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
				log.Logger.Errorf("host: %s, got error %+v", tbl.Host, err)
				gotError = true
			} else {
				log.Logger.Infof("table %s host %s rebalance done", tbl.Table, tbl.Host)
			}
		})
	}
	wg.Wait()
	if gotError {
		return err
	}
	log.Logger.Infof("table %s.%s rebalance done", r.Database, r.Table)
	return
}

func (r *CKRebalance) Close() {
	common.CloseConns(r.Hosts)
}

func (r *CKRebalance) Cleanup() {
	for _, host := range r.Hosts {
		conn := common.GetConnection(host)
		tableName := fmt.Sprintf("tmp_%s", r.Table)
		cleanSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` SYNC", r.Database, tableName)
		log.Logger.Debugf("[%s]%s", host, cleanSql)
		if err := conn.Exec(context.Background(), cleanSql); err != nil {
			log.Logger.Warnf("drop table %s.%s failed: %v", r.Database, tableName, err)
		}
	}
}

func (r *CKRebalance) CreateTemporaryTable() error {
	// if ckman crashed when rebalancing, cleanup tmp table to ensure next rebalance successfully
	r.Cleanup()
	for _, host := range r.Hosts {
		conn := common.GetConnection(host)
		tableName := fmt.Sprintf("tmp_%s", r.Table)
		createSql := fmt.Sprintf("SELECT create_table_query FROM system.tables WHERE database = '%s' AND name = '%s'", r.Database, r.Table)
		rows, err := conn.Query(context.Background(), createSql)
		if err != nil {
			return errors.Wrap(err, host)
		}
		defer rows.Close()
		var create string
		for rows.Next() {
			_ = rows.Scan(&create)
		}
		create = strings.Replace(create, fmt.Sprintf("CREATE TABLE %s.%s", r.Database, r.Table), fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", r.Database, tableName), -1)
		create = strings.ReplaceAll(create, fmt.Sprintf("/%s/", r.Table), fmt.Sprintf("/%s/", tableName)) // replace engine zoopath
		log.Logger.Debug(create)
		if create != "" {
			err = conn.Exec(context.Background(), create)
			if err != nil {
				return errors.Wrap(err, host)
			}
		}
	}
	return nil
}

func (r *CKRebalance) InsertPlan() error {
	max_insert_threads := runtime.NumCPU()*3/4 + 1 // add 1 to ensure threads not zero
	var lastError error
	var wg sync.WaitGroup
	for idx, host := range r.Hosts {
		idx := idx
		host := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			conn := common.GetConnection(host)

			tableName := fmt.Sprintf("tmp_%s", r.Table)
			query := fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s` WHERE %s %% %d = %d SETTINGS max_insert_threads=%d",
				r.Database, tableName, r.Database, r.DistTable, ShardingFunc(r.Shardingkey), len(r.Hosts), idx, max_insert_threads)
			log.Logger.Debugf("[%s]%s", host, query)
			if err := conn.Exec(context.Background(), query); err != nil {
				lastError = errors.Wrap(err, host)
				return
			}

		})
	}
	wg.Wait()
	return lastError
}

func (r *CKRebalance) MoveBack() error {
	conf, err := repository.Ps.GetClusterbyName(r.Cluster)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var lastError error
	for _, host := range r.Hosts {
		host := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			// truncate and detach ori table
			tableName := fmt.Sprintf("tmp_%s", r.Table)
			query := fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", r.Database, r.Table)
			conn := common.GetConnection(host)
			log.Logger.Debugf("[%s]%s", host, query)
			if err = conn.Exec(context.Background(), query); err != nil {
				lastError = errors.Wrap(err, host)
				return
			}

			var partitions []string
			query = fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE database = '%s' AND table = '%s' AND active=1", r.Database, tableName)
			log.Logger.Debugf("[%s]%s", host, query)
			rows, err := conn.Query(context.Background(), query)
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}
			defer rows.Close()
			for rows.Next() {
				var partition string
				if err := rows.Scan(&partition); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
				if partition != "" {
					partitions = append(partitions, partition)
				}
			}

			// copy data
			cmd := fmt.Sprintf("ls -l %sclickhouse/data/%s/%s/ |grep -v total |awk '{print $9}'", r.DataDir, r.Database, tableName)
			sshOpts := common.SshOptions{
				User:             conf.SshUser,
				Password:         conf.SshPassword,
				Port:             conf.SshPort,
				Host:             host,
				NeedSudo:         conf.NeedSudo,
				AuthenticateType: conf.AuthenticateType,
			}
			out, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}
			parts := make([]string, 0)
			for _, file := range strings.Split(out, "\n") {
				file = strings.TrimSpace(strings.TrimSuffix(file, "\r"))
				reg, err := regexp.Compile(`[^_]+(_\d+){3,}$`) //parts name
				if err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
				if reg.MatchString(file) {
					parts = append(parts, file)
				}
			}
			var cmds []string
			for _, part := range parts {
				cmds = append(cmds, fmt.Sprintf("cp -prf %sclickhouse/data/%s/%s/%s %sclickhouse/data/%s/%s/detached/", r.DataDir, r.Database, tableName, part, r.DataDir, r.Database, r.Table))
			}
			_, err = common.RemoteExecute(sshOpts, strings.Join(cmds, ";"))
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}

			for _, partiton := range partitions {
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION '%s'", r.Database, r.Table, partiton)
				log.Logger.Debugf("[%s]%s", host, query)
				if err = conn.Exec(context.Background(), query); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
			}
		})
		wg.Wait()
	}
	return lastError
}
