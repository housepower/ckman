package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
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
// - This query only works for the replicated tables.
// - If inserting to the partition during FETCH(node2)-ATTACH(node2)-DETACH(node1), you get data loss.

// TODO:
// - don't move new partitions
// - [FETCH PARTITION] for replicated tables

type CmdOptions struct {
	ShowVer    bool
	ChHosts    string
	ChAllHosts string
	ChPort     int
	ChUser     string
	ChPassword string
	ChTables   string
	ChDataDir  string
	OsUser     string
	OsPassword string
}

var (
	cmdOps     CmdOptions
	chHosts    []string
	chAllHosts []string
	chTables   []string
	sshConns   map[string]*ssh.Client
	ckConns    map[string]*sql.DB
	locks      map[string]*sync.Mutex
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:   false,
		ChPort:    9000,
		ChDataDir: "/var/lib",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvStringVar(&cmdOps.ChAllHosts, "ch-all-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChTables, "ch-tables")
	common.EnvStringVar(&cmdOps.ChDataDir, "ch-data-dir")
	common.EnvStringVar(&cmdOps.OsUser, "os-user")
	common.EnvStringVar(&cmdOps.OsPassword, "os-password")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ChHosts, "ch-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, one host from each shard")
	flag.StringVar(&cmdOps.ChAllHosts, "ch-all-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, all hosts from all shards")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.StringVar(&cmdOps.ChTables, "ch-tables", cmdOps.ChTables, "a list of comma-separated table names")
	flag.StringVar(&cmdOps.ChDataDir, "ch-data-dir", cmdOps.ChDataDir, "clickhouse data directory")
	flag.StringVar(&cmdOps.OsUser, "os-user", cmdOps.OsUser, "os user")
	flag.StringVar(&cmdOps.OsPassword, "os-password", cmdOps.OsPassword, "os password")
	flag.Parse()
}

func initConns() (err error) {
	sshConns = make(map[string]*ssh.Client)
	ckConns = make(map[string]*sql.DB)
	locks = make(map[string]*sync.Mutex)
	for _, host := range chAllHosts {
		var conn *ssh.Client
		if conn, err = common.SSHConnect(cmdOps.OsUser, cmdOps.OsPassword, host, 22); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		sshConns[host] = conn
		log.Infof("initialized ssh connection to %s", host)
		var db *sql.DB
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
			host, cmdOps.ChPort, "default", cmdOps.ChUser, cmdOps.ChPassword)
		if db, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		ckConns[host] = db
		log.Infof("initialized clickhouse connection to %s", host)
		locks[host] = &sync.Mutex{}
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

// TblPartitions is partitions status of a host. A host never move out and move in at the same ieration.
type TblPartitions struct {
	Host       string
	Partitions map[string]int64
	TotalSize  int64             // total size of partitions
	ToMoveOut  map[string]string // plan to move some partitions out to other hosts
	ToMoveIn   bool              // plan to move some partitions in
}

func getState(table string) (tbls []*TblPartitions, err error) {
	tbls = make([]*TblPartitions, 0)
	for _, host := range chHosts {
		db := ckConns[host]
		var rows *sql.Rows
		query := fmt.Sprintf(`SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE table='%s' AND active=1 GROUP BY partition ORDER BY partition;`, table)
		log.Infof("host %s: query: %s", host, query)
		if rows, err = db.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		tbl := TblPartitions{
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
		tbls = append(tbls, &tbl)
	}
	log.Debugf("table %s state %s", table, pp.Sprint(tbls))
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

func executePlan(table string, tbl *TblPartitions) (err error) {
	if tbl.ToMoveOut == nil {
		return
	}
	for patt, dstHost := range tbl.ToMoveOut {
		srcSshConn := sshConns[tbl.Host]
		srcCkConn := ckConns[tbl.Host]
		dstCkConn := ckConns[dstHost]
		lock := locks[dstHost]
		dstDir := fmt.Sprintf("%s/clickhouse/data/default/%s/detached", cmdOps.ChDataDir, table)
		srcDir := dstDir + "/"

		query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", table, patt)
		log.Infof("host: %s, query: %s", tbl.Host, query)
		if _, err = srcCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}

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

		query = fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", table, patt)
		log.Infof("host: %s, query: %s", dstHost, query)
		if _, err = dstCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			lock.Unlock()
			return
		}
		lock.Unlock()
	}
	return
}

func clearAllHosts(table string) (err error) {
	for host, sshConn := range sshConns {
		cmd := fmt.Sprintf("rm -fr %s/clickhouse/data/default/%s/detached/", cmdOps.ChDataDir, table)
		log.Infof("host: %s, command: %s", host, cmd)
		var out string
		if out, err = common.SSHRun(sshConn, cmd); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		log.Debugf("host: %s, output: %s", host, out)
	}
	return
}

func main() {
	var err error
	initCmdOptions()
	if len(cmdOps.ChHosts) == 0 {
		log.Fatalf("need to specify clickhouse hosts, one from each shard")
	}
	if len(cmdOps.ChAllHosts) == 0 {
		log.Fatalf("need to specify clickhouse hosts, all from all shards")
	}
	if len(cmdOps.ChTables) == 0 {
		log.Fatalf("need to specify clickhouse tables")
	}
	chHosts = strings.Split(cmdOps.ChHosts, ",")
	chAllHosts = strings.Split(cmdOps.ChAllHosts, ",")
	chTables = strings.Split(cmdOps.ChTables, ",")

	if err = initConns(); err != nil {
		log.Fatalf("got error %+v", err)
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
			go func(tbl *TblPartitions) {
				if err := executePlan(table, tbl); err != nil {
					log.Errorf("host: %s, got error %+v", tbl.Host, err)
					gotError = true
				} else {
					log.Infof("table %s host %s rebalance done", table, tbl.Host)
				}
				wg.Done()
			}(tbls[i])
		}
		wg.Wait()
		if gotError {
			os.Exit(-1)
		}
		if err = clearAllHosts(table); err != nil {
			log.Fatalf("got error %+v", err)
		}
		log.Infof("table %s rebalance done", table)
	}
	log.Infof("rebalance done")
	return
}
