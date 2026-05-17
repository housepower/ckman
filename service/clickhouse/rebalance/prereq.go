package rebalance

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

// checkBasicTools verifies that every host has the shell tools each requested
// rebalance policy needs: awk for shardingkey (currently a no-op safety check
// preserved from legacy code) and rsync for non-replicated partition moves.
// Failing fast here saves a long detached/rsync sequence from failing midway.
func checkBasicTools(conf *model.CKManClickHouseConfig, hosts []string, keys []model.RebalanceTables) error {
	var chkrsync, chkawk bool
	for _, key := range keys {
		if chkawk && chkrsync {
			break
		}
		if key.ShardingKey != "" {
			chkawk = true
		} else if !conf.IsReplica {
			chkrsync = true
		}
	}
	for _, host := range hosts {
		opts := common.SshOptions{
			User:             conf.SshUser,
			Password:         conf.SshPassword,
			Port:             conf.SshPort,
			Host:             host,
			NeedSudo:         conf.NeedSudo,
			AuthenticateType: conf.AuthenticateType,
		}

		var cmds []string
		if chkawk {
			cmds = append(cmds, "which awk >/dev/null 2>&1 ;echo $?")
		}
		if chkrsync {
			cmds = append(cmds, "which rsync >/dev/null 2>&1 ;echo $?")
		}
		for _, cmd := range cmds {
			output, err := common.RemoteExecute(opts, cmd)
			if err != nil {
				return err
			}
			if strings.TrimSuffix(output, "\n") != "0" {
				return errors.Errorf("excute cmd:[%s] on %s failed", cmd, host)
			}
		}
	}
	return nil
}

// checkDiskSpace picks the host (other than exceptHost) with the most free
// space and returns its address, after verifying it has at least as much free
// space as exceptHost's total MergeTree usage. Used by the exceptMaxShard
// downsizing path before moving data off the soon-to-be-removed shard.
func checkDiskSpace(hosts []string, exceptHost string) (string, error) {
	var needSpace, maxLeftSpace uint64
	var target string
	query := `SELECT sum(total_bytes)
FROM system.tables
WHERE match(engine, 'MergeTree') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))
SETTINGS skip_unavailable_shards = 1`
	log.Logger.Debugf("[%s]%s", exceptHost, query)
	conn := common.GetConnection(exceptHost)
	rows, err := conn.Query(query)
	if err != nil {
		return "", errors.Wrap(err, exceptHost)
	}
	for rows.Next() {
		_ = rows.Scan(&needSpace)
	}
	query = "SELECT free_space FROM system.disks"
	for _, host := range hosts {
		conn = common.GetConnection(host)
		log.Logger.Debugf("[%s]%s", host, query)
		rows, err := conn.Query(query)
		if err != nil {
			return "", errors.Wrap(err, exceptHost)
		}
		var freeSpace uint64
		for rows.Next() {
			_ = rows.Scan(&freeSpace)
		}
		if maxLeftSpace*2 < freeSpace {
			target = host
			maxLeftSpace = freeSpace
		}
	}
	if maxLeftSpace <= needSpace {
		return "", fmt.Errorf("need %s space on the disk, but not enough", common.ConvertDisk(needSpace))
	}
	return target, nil
}

// moveExceptToOthers drains all data from the about-to-be-removed shard
// (except) into target via INSERT ... SELECT remote(), then truncates the
// source. Used by the exceptMaxShard downsizing path before a per-table
// rebalance on the remaining shards.
func moveExceptToOthers(conf *model.CKManClickHouseConfig, except, target, database, table string) error {
	maxInsertThreads := runtime.NumCPU()*3/4 + 1
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM remote('%s', '%s', '%s', '%s', '%s') SETTINGS max_insert_threads=%d,max_execution_time=0",
		database, table, except, database, table, conf.User, conf.Password, maxInsertThreads)
	log.Logger.Debugf("[%s] %s", target, query)
	conn := common.GetConnection(target)
	if err := conn.Exec(query); err != nil {
		return err
	}
	query = fmt.Sprintf("TRUNCATE TABLE `%s`.`%s` %s", database, table, common.WithAlterSync(conf.Version))
	log.Logger.Debugf("[%s] %s", except, query)
	conn = common.GetConnection(except)
	return conn.Exec(query)
}
