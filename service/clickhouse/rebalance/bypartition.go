package rebalance

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

// TblPartitions captures one host's view of a table's partitions: the set
// known to be present, the total bytes they occupy, and the plan-time decision
// of which partitions to move out (to which dst host) or whether to accept
// incoming moves. A single host never both moves out and moves in within one
// planning iteration.
type TblPartitions struct {
	Table      string
	Host       string
	ZooPath    string // zoo-path with macros substituted
	Partitions map[string]uint64
	TotalSize  uint64            // total size of partitions
	ToMoveOut  map[string]string // patt -> dstHost
	ToMoveIn   bool
}

// ByPartition moves whole partitions across hosts. On replicated clusters it
// is metadata-only (FETCH PARTITION via ZooKeeper); on non-replicated ones it
// detaches, rsyncs the part files, and re-attaches.
type ByPartition struct{}

func (ByPartition) Name() string { return model.RebalancePolicyPartition }

// Validate is a no-op for partition rebalance: there are no per-table
// preconditions beyond the cluster-wide tool checks done in Run.
func (ByPartition) Validate(*Rebalancer, *common.Conn) error { return nil }

func (ByPartition) Run(r *Rebalancer) error {
	if err := r.InitCKConns(false); err != nil {
		log.Logger.Errorf("[rebalance]got error %+v", err)
		return err
	}
	if err := r.getRepTables(); err != nil {
		log.Logger.Errorf("[rebalance]got error %+v", err)
		return err
	}
	if err := r.doRebalanceByPart(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		return err
	}
	log.Logger.Infof("rebalance done")
	return nil
}

// getRepTables fetches each host's local zookeeper_path for the target table.
// Used by the replicated-cluster execution path so FETCH PARTITION can pull
// from the right znode rather than via an OS-level copy.
//
// NOTE (refactor): the pre-refactor code did `defer rows.Close()` inside the
// for-host loop, which accumulated open Rows handles until the function
// returned. We now close per iteration to avoid that fd churn. Functionally
// equivalent in normal flow, slightly better under failure.
func (r *Rebalancer) getRepTables() error {
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
		rows, err := conn.Query(query)
		if err != nil {
			return errors.Wrapf(err, "")
		}
		var zookeeperPath string
		for rows.Next() {
			_ = rows.Scan(&zookeeperPath)
			if _, ok := r.RepTables[host]; !ok {
				r.RepTables[host] = zookeeperPath
			}
		}
		rows.Close()
	}
	return nil
}

// initSSHConns smoke-tests host-to-host passwordless SSH access (and sudo read
// of the data directory) used by the rsync fallback. If this fails the
// non-replicated rebalance path will be skipped per-table rather than failing
// the whole run; replicated clusters don't call it at all.
func (r *Rebalancer) initSSHConns() error {
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
			out, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				return err
			}
			log.Logger.Debugf("[rebalance]host: %s, output: %s", srcHost, out)
		}
	}
	return nil
}

// getPartState lists active partitions per host with their byte sizes,
// deliberately skipping each host's newest partition (still receiving inserts).
//
// NOTE (refactor): same per-iteration `rows.Close()` tightening as
// getRepTables; the pre-refactor code stacked `defer rows.Close()` across
// every host in the loop.
func (r *Rebalancer) getPartState() ([]*TblPartitions, error) {
	tbls := make([]*TblPartitions, 0)
	for _, host := range r.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			return nil, fmt.Errorf("[rebalance]can't get connection: %s", host)
		}
		// Skip the newest partition on each host since into which there could by ongoing insertions.
		query := fmt.Sprintf(`WITH (SELECT argMax(partition, modification_time) FROM system.parts WHERE database='%s' AND table='%s') AS latest_partition SELECT partition, sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1 AND partition!=latest_partition GROUP BY partition ORDER BY partition;`, r.Database, r.Table, r.Database, r.Table)
		log.Logger.Infof("[rebalance]host %s: query: %s", host, query)
		rows, err := conn.Query(query)
		if err != nil {
			return nil, errors.Wrapf(err, "")
		}
		tbl := TblPartitions{
			Table:      fmt.Sprintf("%s.%s", r.Database, r.Table),
			Host:       host,
			Partitions: make(map[string]uint64),
		}
		for rows.Next() {
			var patt string
			var compressed uint64
			if err := rows.Scan(&patt, &compressed); err != nil {
				rows.Close()
				return nil, errors.Wrapf(err, "")
			}
			tbl.Partitions[patt] = compressed
			tbl.TotalSize += compressed
		}
		rows.Close()
		if zoopath, ok := r.RepTables[host]; ok {
			tbl.ZooPath = zoopath
		}
		tbls = append(tbls, &tbl)
	}
	log.Logger.Infof("[rebalance]table %s state %s", r.Table, pp.Sprint(tbls))
	return tbls, nil
}

// generatePartPlan greedily picks the largest host and the smallest host each
// iteration and tries to move one of the larger's partitions onto the smaller,
// requiring max >= min + 2*size so the move actually reduces imbalance rather
// than just swapping who-has-most. Loop exits when no further valid move
// exists or every host has been touched on at least one side.
func (r *Rebalancer) generatePartPlan(tbls []*TblPartitions) {
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

// executePartPlan executes one host's outgoing partition moves. On replicated
// clusters this is metadata-only (FETCH/ATTACH/DROP through ZooKeeper); on
// non-replicated it detaches the source, rsyncs the files, and re-attaches on
// the destination. The dst-host lock is released via defer in either helper to
// guarantee no stranded locks on error.
func (r *Rebalancer) executePartPlan(tbl *TblPartitions) error {
	if tbl.ToMoveOut == nil {
		return nil
	}
	if tbl.ZooPath != "" {
		// 带副本集群
		for patt, dstHost := range tbl.ToMoveOut {
			if err := r.fetchPartitionOnReplica(tbl.Table, patt, tbl.ZooPath, dstHost); err != nil {
				return err
			}
			srcChConn := common.GetConnection(tbl.Host)
			if srcChConn == nil {
				return fmt.Errorf("[rebalance]can't get connection: %s", tbl.Host)
			}
			query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tbl.Table, patt)
			log.Logger.Infof("[rebalance]host %s: query: %s", tbl.Host, query)
			if err := srcChConn.Exec(query); err != nil {
				return errors.Wrapf(err, "")
			}
		}
		return nil
	}
	//不带副本集群， 公钥认证集群做不了
	if r.sshErr != nil {
		log.Logger.Warnf("[rebalance]skip execution for %s due to previous SSH error", tbl.Table)
		return nil
	}
	for patt, dstHost := range tbl.ToMoveOut {
		srcCkConn := common.GetConnection(tbl.Host)
		dstCkConn := common.GetConnection(dstHost)
		if srcCkConn == nil || dstCkConn == nil {
			return fmt.Errorf("[rebalance]can't get connection: %s & %s", tbl.Host, dstHost)
		}
		tableName := strings.Split(tbl.Table, ".")[1]
		dstDir := filepath.Join(r.DataDir, fmt.Sprintf("clickhouse/data/%s/%s/detached", r.Database, tableName))
		srcDir := dstDir + "/"

		query := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("[rebalance]host: %s, query: %s", tbl.Host, query)
		if err := srcCkConn.Exec(query); err != nil {
			return errors.Wrapf(err, "")
		}

		if err := r.rsyncAndAttach(tbl, patt, srcDir, dstDir, dstHost, dstCkConn); err != nil {
			return err
		}

		query = fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s'", tbl.Table, patt)
		log.Logger.Infof("[rebalance]host: %s, query: %s", tbl.Host, query)
		if err := srcCkConn.Exec(query); err != nil {
			return errors.Wrapf(err, "")
		}
	}
	return nil
}

// fetchPartitionOnReplica holds the dstHost lock for the whole
// drop-detached/fetch/attach sequence so concurrent moves to the same dst
// serialize correctly. The lock is always released via defer.
func (r *Rebalancer) fetchPartitionOnReplica(table, patt, zooPath, dstHost string) error {
	lock := r.locks[dstHost]
	lock.Lock()
	defer lock.Unlock()

	dstChConn := common.GetConnection(dstHost)
	if dstChConn == nil {
		return fmt.Errorf("[rebalance]can't get connection: %s", dstHost)
	}
	queries := []string{
		fmt.Sprintf("ALTER TABLE %s DROP DETACHED PARTITION '%s' ", table, patt),
		fmt.Sprintf("ALTER TABLE %s FETCH PARTITION '%s' FROM '%s'", table, patt, zooPath),
		fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", table, patt),
	}
	for _, query := range queries {
		log.Logger.Infof("[rebalance]host %s: query: %s", dstHost, query)
		if err := dstChConn.Exec(query); err != nil {
			return errors.Wrapf(err, "")
		}
	}
	return nil
}

// rsyncAndAttach performs rsync-from-source plus ATTACH-on-destination under
// the dstHost lock. defer Unlock guarantees the lock is released on any return
// path.
func (r *Rebalancer) rsyncAndAttach(tbl *TblPartitions, patt, srcDir, dstDir, dstHost string, dstCkConn *common.Conn) error {
	lock := r.locks[dstHost]
	lock.Lock()
	defer lock.Unlock()

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
	out, err := common.RemoteExecute(sshOpts, strings.Join(cmds, ";"))
	if err != nil {
		return err
	}
	log.Logger.Debugf("[rebalance]host: %s, output: %s", tbl.Host, out)

	query := fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION '%s'", tbl.Table, patt)
	log.Logger.Infof("[rebalance]host: %s, query: %s", dstHost, query)
	if err := dstCkConn.Exec(query); err != nil {
		return errors.Wrapf(err, "")
	}
	return nil
}

// doRebalanceByPart orchestrates one table's partition rebalance: collect
// state, plan moves, then execute per-host in parallel. The first error
// returned by any worker wins and is propagated to the caller.
//
// Three outer phases are reported via setStep (GetState / GenPlan / Execute);
// the inner helpers are pure compute or per-host work and don't emit their
// own step transitions.
func (r *Rebalancer) doRebalanceByPart() error {
	if !r.IsReplica {
		if r.sshErr = r.initSSHConns(); r.sshErr != nil {
			log.Logger.Warnf("[rebalance]failed to init ssh connections, error: %+v", r.sshErr)
		}
	}
	r.setStep(model.StepPartGetState)
	tbls, err := r.getPartState()
	if err != nil {
		log.Logger.Errorf("[rebalance]got error %+v", err)
		return err
	}
	r.setStep(model.StepPartGenPlan)
	r.generatePartPlan(tbls)
	r.setStep(model.StepPartExecute)

	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	for i := 0; i < len(tbls); i++ {
		tbl := tbls[i]
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			if e := r.executePartPlan(tbl); e != nil {
				log.Logger.Errorf("[rebalance]host: %s, got error %+v", tbl.Host, e)
				mu.Lock()
				if firstErr == nil {
					firstErr = e
				}
				mu.Unlock()
			} else {
				log.Logger.Infof("[rebalance]table %s host %s rebalance done", tbl.Table, tbl.Host)
			}
		})
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	log.Logger.Infof("[rebalance]table %s.%s rebalance done", r.Database, r.Table)
	return nil
}
