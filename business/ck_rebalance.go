package business

import (
	"database/sql"
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
	Hosts      []string
	Port       int
	User       string
	Password   string
	Databases  []string
	DataDir    string
	DBTables   map[string][]string
	RepTables  map[string]map[string]string
	OsUser     string
	OsPassword string
	OsPort     int
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

func (this *CKRebalance) InitCKConns() (err error) {
	locks = make(map[string]*sync.Mutex)
	for _, host := range this.Hosts {
		_, err = common.ConnectClickHouse(host, this.Port, model.ClickHouseDefaultDB, this.User, this.Password)
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
		locks[host] = &sync.Mutex{}
	}
	return
}

func (this *CKRebalance) GetTables() (err error) {
	host := this.Hosts[0]
	db := common.GetConnection(host)
	if db == nil {
		return fmt.Errorf("can't get connection: %s", host)
	}
	if this.Databases, this.DBTables, err = common.GetMergeTreeTables("MergeTree", db); err != nil {
		return
	}
	return
}

func (this *CKRebalance) GetRepTables() (err error) {
	for _, host := range this.Hosts {
		for _, database := range this.Databases {
			db := common.GetConnection(host)
			if db == nil {
				return fmt.Errorf("can't get connection: %s", host)
			}
			query := fmt.Sprintf("SELECT table, zookeeper_path FROM system.replicas WHERE database='%s'", database)
			log.Logger.Infof("host %s: query: %s", host, query)
			var rows *sql.Rows
			if rows, err = db.Query(query); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			defer rows.Close()
			var table, zookeeper_path string
			for rows.Next() {
				_ = rows.Scan(&table, &zookeeper_path)
				var host2ZooPath map[string]string
				var ok bool
				tablename := fmt.Sprintf("%s.%s", database, table)
				if host2ZooPath, ok = this.RepTables[tablename]; !ok {
					host2ZooPath = make(map[string]string)
					this.RepTables[tablename] = host2ZooPath
				}
				host2ZooPath[host] = zookeeper_path
			}
		}
	}
	return
}

func (this *CKRebalance) InitSshConns(database string) (err error) {
	// Validate if one can login from any host to another host without password, and read the data directory.
	for _, srcHost := range this.Hosts {
		for _, dstHost := range this.Hosts {
			if srcHost == dstHost {
				continue
			}
			cmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=false %s ls %s/clickhouse/data/%s", dstHost, this.DataDir, database)
			log.Logger.Infof("host: %s, command: %s", srcHost, cmd)
			sshOpts := common.SshOptions{
				User:             this.OsUser,
				Password:         this.OsPassword,
				Port:             this.OsPort,
				Host:             srcHost,
				NeedSudo:         true,
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

func (this *CKRebalance) GetState(database string, table string) (tbls []*TblPartitions, err error) {
	tbls = make([]*TblPartitions, 0)
	for _, host := range this.Hosts {
		db := common.GetConnection(host)
		if db == nil {
			err = fmt.Errorf("can't get connection: %s", host)
			return
		}
		var rows *sql.Rows
		// Skip the newest partition on each host since into which there could by ongoing insertions.
		query := fmt.Sprintf(`WITH (SELECT argMax(partition, modification_time) FROM system.parts WHERE database='%s' AND table='%s') AS latest_partition SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1 AND partition!=latest_partition GROUP BY partition ORDER BY partition;`, database, table, database, table)
		log.Logger.Infof("host %s: query: %s", host, query)
		if rows, err = db.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		tbl := TblPartitions{
			Table:      fmt.Sprintf("%s.%s", database, table),
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
		tablename := fmt.Sprintf("%s.%s", database, table)
		if host2ZooPath, ok := this.RepTables[tablename]; ok {
			tbl.ZooPath = host2ZooPath[host]
		}
		tbls = append(tbls, &tbl)
	}
	log.Logger.Infof("table %s state %s", table, pp.Sprint(tbls))
	return
}

func (this *CKRebalance) GeneratePlan(tablename string, tbls []*TblPartitions) {
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
	log.Logger.Infof("table %s plan %s", tablename, pp.Sprint(tbls))
}

func (this *CKRebalance) ExecutePlan(database string, tbl *TblPartitions) (err error) {
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
				if _, err = dstChConn.Exec(query); err != nil {
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
			if _, err = srcChConn.Exec(query); err != nil {
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
		dstDir := filepath.Join(this.DataDir, fmt.Sprintf("clickhouse/data/%s/%s/detached", database, tableName))
		srcDir := dstDir + "/"

		query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("host: %s, query: %s", tbl.Host, query)
		if _, err = srcCkConn.Exec(query); err != nil {
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
			User:             this.OsUser,
			Password:         this.OsPassword,
			Port:             this.OsPort,
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
		if _, err = dstCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			lock.Unlock()
			return
		}
		lock.Unlock()

		query = fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("host: %s, query: %s", tbl.Host, query)
		if _, err = srcCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}

func (this *CKRebalance) DoRebalance() (err error) {
	for _, database := range this.Databases {
		tables := this.DBTables[database]
		for _, table := range tables {
			tablename := fmt.Sprintf("%s.%s", database, table)
			if _, ok := this.RepTables[tablename]; !ok {
				// initialize SSH connections only if there are some non-replicated tables
				if sshErr = this.InitSshConns(database); sshErr != nil {
					log.Logger.Warnf("failed to init ssh connections, error: %+v", sshErr)
				}
				break
			}
		}
		for _, table := range tables {
			var tbls []*TblPartitions
			if tbls, err = this.GetState(database, table); err != nil {
				log.Logger.Errorf("got error %+v", err)
				return err
			}
			this.GeneratePlan(fmt.Sprintf("%s.%s", database, table), tbls)

			var gotError bool
			for i := 0; i < len(tbls); i++ {
				tbl := tbls[i]
				_ = common.Pool.Submit(func() {
					if err := this.ExecutePlan(database, tbl); err != nil {
						log.Logger.Errorf("host: %s, got error %+v", tbl.Host, err)
						gotError = true
					} else {
						log.Logger.Infof("table %s host %s rebalance done", tbl.Table, tbl.Host)
					}
				})
			}
			common.Pool.Wait()
			if gotError {
				return err
			}
			log.Logger.Infof("table %s rebalance done", table)
		}
	}
	return nil
}
