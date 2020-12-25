package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gitlab.eoitek.net/EOI/ckman/common"
	"golang.org/x/crypto/ssh"
)

// Rebalance the whole cluster. It queries every shard to get partitions size, caculates a plan, for each selected partition, detach from source node, rsync to dest node and attach it.
// https://clickhouse.tech/docs/en/sql-reference/statements/alter/#synchronicity-of-alter-queries

// [FETCH PARTITION](https://clickhouse.tech/docs/en/sql-reference/statements/alter/partition/#alter_fetch-partition) drawbacks:
// - If inserting to the partition during FETCH(node2)-ATTACH(node2)-DROP(node1), you get data loss.
// - Non-replicated table doesn't support this command.

type CmdOptions struct {
	ShowVer    bool
	ChHosts    string
	ChPort     int
	ChUser     string
	ChPassword string
	ChDatabase string
	ChDataDir  string
	OsUser     string
	OsPassword string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	chHosts        []string
	chRepTables    map[string]map[string]string //table -> host -> zookeeper_path
	chTables       []string
	sshConns       map[string]*ssh.Client
	sshErr         error
	chConns        map[string]*sql.DB
	locks          map[string]*sync.Mutex
	globalPool     *common.WorkerPool
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:    false,
		ChPort:     9000,
		ChDatabase: "default",
		ChDataDir:  "/var/lib",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChDatabase, "ch-database")
	common.EnvStringVar(&cmdOps.ChDataDir, "ch-data-dir")
	common.EnvStringVar(&cmdOps.OsUser, "os-user")
	common.EnvStringVar(&cmdOps.OsPassword, "os-password")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ChHosts, "ch-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, one host from each shard")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.StringVar(&cmdOps.ChDatabase, "ch-database", cmdOps.ChDatabase, "clickhouse database")
	flag.StringVar(&cmdOps.ChDataDir, "ch-data-dir", cmdOps.ChDataDir, "clickhouse data directory, required for rebalancing non-replicated tables")
	flag.StringVar(&cmdOps.OsUser, "os-user", cmdOps.OsUser, "os user, required for rebalancing non-replicated tables")
	flag.StringVar(&cmdOps.OsPassword, "os-password", cmdOps.OsPassword, "os password")
	flag.Parse()
}

func initChConns() (err error) {
	chConns = make(map[string]*sql.DB)
	locks = make(map[string]*sync.Mutex)
	for _, host := range chHosts {
		var db *sql.DB
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
			host, cmdOps.ChPort, cmdOps.ChDatabase, cmdOps.ChUser, cmdOps.ChPassword)
		if db, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		chConns[host] = db
		log.Infof("initialized clickhouse connection to %s", host)
		locks[host] = &sync.Mutex{}
	}
	return
}

func initSshConns() (err error) {
	sshConns = make(map[string]*ssh.Client)
	for _, host := range chHosts {
		var conn *ssh.Client
		if conn, err = common.SSHConnect(cmdOps.OsUser, cmdOps.OsPassword, host, 22); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		sshConns[host] = conn
		log.Infof("initialized ssh connection to %s", host)
	}
	// Validate if one can login from any host to another host without password, and read the data directory.
	for _, srcHost := range chHosts {
		for _, dstHost := range chHosts {
			if srcHost == dstHost {
				continue
			}
			sshConn := sshConns[srcHost]
			cmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=false %s ls %s/clickhouse/data/default", dstHost, cmdOps.ChDataDir)
			log.Infof("host: %s, command: %s", srcHost, cmd)
			var out string
			if out, err = common.SSHRun(sshConn, cmd); err != nil {
				err = errors.Wrapf(err, "output: %s", out)
				return
			}
			log.Debugf("host: %s, output: %s", srcHost, out)
		}
	}
	return
}

func getRepTables() (err error) {
	chRepTables = make(map[string]map[string]string)
	for _, host := range chHosts {
		db := chConns[host]
		query := fmt.Sprintf("SELECT table, zookeeper_path FROM system.replicas WHERE database='%s'", cmdOps.ChDatabase)
		log.Infof("host %s: query: %s", host, query)
		var rows *sql.Rows
		if rows, err = db.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		var table, zookeeper_path string
		for rows.Next() {
			rows.Scan(&table, &zookeeper_path)
			var host2ZooPath map[string]string
			var ok bool
			if host2ZooPath, ok = chRepTables[table]; !ok {
				host2ZooPath = make(map[string]string)
				chRepTables[table] = host2ZooPath
			}
			host2ZooPath[host] = zookeeper_path
		}
	}
	return
}

func getTables() (err error) {
	host := chHosts[0]
	db := chConns[host]
	var rows *sql.Rows
	query := fmt.Sprintf("SELECT DISTINCT name FROM system.tables WHERE (database = '%s') AND (engine LIKE '%%MergeTree%%')", cmdOps.ChDatabase)
	log.Infof("host %s: query: %s", host, query)
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		chTables = append(chTables, name)
	}
	return
}

// TblPartitions is partitions status of a host. A host never move out and move in at the same ieration.
type TblPartitions struct {
	Table      string
	Host       string
	ZooPath    string // zoo-path with macros substituted
	Partitions map[string]int64
	TotalSize  int64             // total size of partitions
	ToMoveOut  map[string]string // plan to move some partitions out to other hosts
	ToMoveIn   bool              // plan to move some partitions in
}

func getState(table string) (tbls []*TblPartitions, err error) {
	tbls = make([]*TblPartitions, 0)
	for _, host := range chHosts {
		db := chConns[host]
		var rows *sql.Rows
		// Skip the newest partition on each host since into which there could by ongoing insertons.
		query := fmt.Sprintf(`WITH (SELECT argMax(partition, modification_time) FROM system.parts WHERE database='%s' AND table='%s') AS latest_partition SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1 AND partition!=latest_partition GROUP BY partition ORDER BY partition;`, cmdOps.ChDatabase, table, cmdOps.ChDatabase, table)
		log.Infof("host %s: query: %s", host, query)
		if rows, err = db.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		tbl := TblPartitions{
			Table:      table,
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
		if host2ZooPath, ok := chRepTables[table]; ok {
			tbl.ZooPath = host2ZooPath[host]
		}
		tbls = append(tbls, &tbl)
	}
	log.Infof("table %s state %s", table, pp.Sprint(tbls))
	return
}

func generatePlan(table string, tbls []*TblPartitions) {
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
	log.Infof("table %s plan %s", table, pp.Sprint(tbls))
}

func executePlan(tbl *TblPartitions) (err error) {
	if tbl.ToMoveOut == nil {
		return
	}
	if tbl.ZooPath != "" {
		for patt, dstHost := range tbl.ToMoveOut {
			lock := locks[dstHost]

			// There could be multiple executions on the same dest node and partition.
			lock.Lock()
			dstCkConn := chConns[dstHost]
			dstQuires := []string{
				fmt.Sprintf("ALTER TABLE %s FETCH PARTITION '%s' FROM '%s'", tbl.Table, patt, tbl.ZooPath),
				fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt),
			}
			for _, query := range dstQuires {
				if _, err = dstCkConn.Exec(query); err != nil {
					err = errors.Wrapf(err, "")
					return
				}
			}
			lock.Unlock()

			srcCkConn := chConns[tbl.Host]
			query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tbl.Table, patt)
			if _, err = srcCkConn.Exec(query); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
		return
	}
	if sshErr != nil {
		log.Warnf("skip execution for %s due to previous SSH error", tbl.Table)
		return
	}
	for patt, dstHost := range tbl.ToMoveOut {
		srcSshConn := sshConns[tbl.Host]
		srcCkConn := chConns[tbl.Host]
		dstCkConn := chConns[dstHost]
		lock := locks[dstHost]
		dstDir := filepath.Join(cmdOps.ChDataDir, fmt.Sprintf("clickhouse/data/default/%s/detached", tbl.Table))
		srcDir := dstDir + "/"

		query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", tbl.Table, patt)
		log.Infof("host: %s, query: %s", tbl.Host, query)
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
		for _, cmd := range cmds {
			log.Infof("host: %s, command: %s", tbl.Host, cmd)
			var out string
			if out, err = common.SSHRun(srcSshConn, cmd); err != nil {
				err = errors.Wrapf(err, "output: %s", out)
				lock.Unlock()
				return
			}
			log.Debugf("host: %s, output: %s", tbl.Host, out)
		}

		query = fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt)
		log.Infof("host: %s, query: %s", dstHost, query)
		if _, err = dstCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			lock.Unlock()
			return
		}
		lock.Unlock()

		query = fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s'", tbl.Table, patt)
		if _, err = srcCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}

func main() {
	var err error
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if len(cmdOps.ChHosts) == 0 {
		log.Fatalf("need to specify clickhouse hosts, one from each shard")
	}
	chHosts = strings.Split(cmdOps.ChHosts, ",")

	globalPool = common.NewWorkerPool(len(chHosts), len(chHosts))
	if err = initChConns(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	if err = getTables(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	if err = getRepTables(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	for _, table := range chTables {
		if _, ok := chRepTables[table]; !ok {
			// initialize SSH connections only if there are some non-replicated tables
			if sshErr = initSshConns(); sshErr != nil {
				log.Warnf("failed to init ssh connections, error: %+v", sshErr)
			}
			break
		}
	}
	for _, table := range chTables {
		var tbls []*TblPartitions
		if tbls, err = getState(table); err != nil {
			log.Fatalf("got error %+v", err)
		}
		generatePlan(table, tbls)
		wg := sync.WaitGroup{}
		wg.Add(len(tbls))
		var gotError bool
		for i := 0; i < len(tbls); i++ {
			tbl := tbls[i]
			globalPool.Submit(func() {
				if err := executePlan(tbl); err != nil {
					log.Errorf("host: %s, got error %+v", tbl.Host, err)
					gotError = true
				} else {
					log.Infof("table %s host %s rebalance done", tbl.Table, tbl.Host)
				}
				wg.Done()
			})
		}
		wg.Wait()
		if gotError {
			os.Exit(-1)
		}
		log.Infof("table %s rebalance done", table)
	}
	log.Infof("rebalance done")
	return
}
