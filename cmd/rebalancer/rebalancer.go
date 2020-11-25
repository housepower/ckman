package main

import (
	"database/sql"
	"fmt"
	"sort"
	"sync"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gitlab.eoitek.net/EOI/ckman/common"
	"golang.org/x/crypto/ssh"
)

// Rebalance the whole cluster.
var (
	ckHosts  = []string{"192.168.101.106", "192.168.101.108", "192.168.101.110"}
	port     = 9000
	username = "eoi"
	password = "123456"
	dataDir  = "/data01/clickhouse"
	table    = "nginx_access_log22"
	osUser   = "root"
	osPass   = "Eoi123456!"

	sizeSQLTemplate = `SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE table='%s' AND active=1 GROUP BY partition ORDER BY partition;`

	sshConns map[string]*ssh.Client
	ckConns  map[string]*sql.DB
	locks    map[string]*sync.Mutex
)

func initConns() (err error) {
	sshConns = make(map[string]*ssh.Client)
	ckConns = make(map[string]*sql.DB)
	locks = make(map[string]*sync.Mutex)
	for _, host := range ckHosts {
		var conn *ssh.Client
		if conn, err = common.SSHConnect(osUser, osPass, host, 22); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		sshConns[host] = conn
		log.Infof("initialized ssh connection to %s", host)
		var db *sql.DB
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
			host, port, "default", username, password)
		if db, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		ckConns[host] = db
		log.Infof("initialized clickhouse connection to %s", host)
		locks[host] = &sync.Mutex{}
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

func getState() (tbls []TblPartitions, err error) {
	tbls = make([]TblPartitions, 0)
	for _, host := range ckHosts {
		db := ckConns[host]
		var rows *sql.Rows
		query := fmt.Sprintf(sizeSQLTemplate, table)
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
		tbls = append(tbls, tbl)
	}
	log.Infof("tbls: %#v", tbls)
	return
}

func generatePlan(tbls []TblPartitions) {
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
		minTbl := &tbls[minIdx]
		maxTbl := &tbls[maxIdx]
		var found bool
		for patt, pattSize := range maxTbl.Partitions {
			if maxTbl.TotalSize >= minTbl.TotalSize+2*pattSize {
				minTbl.TotalSize += pattSize
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
			break
		}
	}
	log.Infof("plan: %#v", tbls)
}

func executePlan(tbl *TblPartitions) (err error) {
	if tbl.ToMoveOut == nil {
		return
	}
	for patt, dstHost := range tbl.ToMoveOut {
		srcSshConn := sshConns[tbl.Host]
		srcCkConn := ckConns[tbl.Host]
		dstCkConn := ckConns[dstHost]
		lock := locks[dstHost]
		srcDir := fmt.Sprintf("%s/data/default/%s/detached/", dataDir, table)
		dstDir := fmt.Sprintf("%s/data/default/%s/detached", dataDir, table)

		query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", table, patt)
		log.Infof("host: %s, query: %s", tbl.Host, query)
		if _, err = srcCkConn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}

		lock.Lock()
		cmds := []string{
			fmt.Sprintf("rsync -avp %s %s:%s", srcDir, dstHost, dstDir),
			fmt.Sprintf("rm -r %s", srcDir),
		}
		for _, cmd := range cmds {
			log.Infof("host: %s, command: %s", tbl.Host, cmd)
			var out string
			if out, err = common.SSHRun(srcSshConn, cmd); err != nil {
				err = errors.Wrapf(err, "")
				lock.Unlock()
				return
			}
			log.Infof("host: %s, output: %s", tbl.Host, out)
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

func main() {
	var err error
	var tbls []TblPartitions
	if err = initConns(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	if tbls, err = getState(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	generatePlan(tbls)
	wg := sync.WaitGroup{}
	wg.Add(len(tbls))
	for i := 0; i < len(tbls); i++ {
		go func(tbl *TblPartitions) {
			if err := executePlan(tbl); err != nil {
				log.Errorf("host: %s, got error %+v", tbl.Host, err)
			} else {
				log.Infof("host: %s, rebalance done", tbl.Host)
			}
			wg.Done()
		}(&tbls[i])
	}
	wg.Wait()
	log.Infof("rebalance done")
	return
}
