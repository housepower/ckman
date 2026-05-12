package backup

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

// backupConnOpts returns ConnetOption presets for backup/restore connections:
//   - MaxRead 6h: backup of large tables can stream for hours
//   - max_execution_time=0: server-side per-connection setting; BACKUP/RESTORE
//     statements don't accept inline SETTINGS so this is the only way to disable it.
//   - BypassPool: backup 用完即关，不让此连接进 common.ConnectPool 被普通业务复用
//     （其 ReadTimeout 与 Settings 与普通用法不兼容）。
func backupConnOpts(cc model.CKManClickHouseConfig) model.ConnetOption {
	return cc.GetConnOption(
		model.WithMaxRead(21600),
		model.WithSettings(clickhouse.Settings{
			"max_execution_time": uint64(0),
		}),
		model.WithBypassPool(true),
	)
}

// ClickHouseAdapter 提供 Executor 所需的真实实现 hook。
//
// getCluster / dial / sshExec / queryDataPaths 是注入点，便于单测；NewClickHouseAdapter 用真实实现。
type ClickHouseAdapter struct {
	getCluster     func(string) (model.CKManClickHouseConfig, error)
	dial           func(host string, opt model.ConnetOption) (*common.Conn, error)
	sshExec        func(opts common.SshOptions, cmd string) (string, error)
	queryDataPaths func(c *shardConn, db, table string) ([]string, error)
}

// NewClickHouseAdapter 真实环境工厂。
func NewClickHouseAdapter() *ClickHouseAdapter {
	a := &ClickHouseAdapter{
		getCluster: repository.Ps.GetClusterbyName,
		dial: func(host string, opt model.ConnetOption) (*common.Conn, error) {
			return common.ConnectClickHouse(host, model.ClickHouseDefaultDB, opt)
		},
		sshExec: common.RemoteExecute,
	}
	a.queryDataPaths = a.realQueryTableDataPaths
	return a
}

// ConnFactory 给 Executor.connFactory 用：每 shard 拿第一个可达 replica，缓存 *common.Conn 到 shardConn。
func (a *ClickHouseAdapter) ConnFactory(cluster string) ([]*shardConn, error) {
	cc, err := a.getCluster(cluster)
	if err != nil {
		return nil, err
	}
	opt := backupConnOpts(cc)
	var conns []*shardConn
	for idx, shard := range cc.Shards {
		var got *shardConn
		for _, replica := range shard.Replicas {
			conn, derr := a.dial(replica.Ip, opt)
			if derr != nil {
				log.Logger.Warnf("[backup] shard %d replica %s unreachable: %v", idx, replica.Ip, derr)
				continue
			}
			got = &shardConn{host: replica.Ip, conn: conn}
			break
		}
		if got == nil {
			return nil, fmt.Errorf("no reachable replica in shard %d", idx)
		}
		conns = append(conns, got)
	}
	return conns, nil
}

// ListPartitions 列指定表 partition <= beforeYYYYMMDD 的所有分区（通过 cluster 视图跨节点查询）。
func (a *ClickHouseAdapter) ListPartitions(c *shardConn, db, table, beforeYYYYMMDD string) ([]string, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("ListPartitions: nil conn for host %s", func() string {
			if c != nil {
				return c.host
			}
			return "<nil>"
		}())
	}
	query := fmt.Sprintf(
		"SELECT DISTINCT partition FROM system.parts WHERE partition <= '%s' AND database = '%s' AND `table` = '%s' AND active = 1 ORDER BY partition",
		strings.ReplaceAll(beforeYYYYMMDD, "'", "''"),
		strings.ReplaceAll(db, "'", "''"),
		strings.ReplaceAll(table, "'", "''"),
	)
	rows, err := c.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var partitions []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}

// ListAllPartitions 列指定表全部 active 分区（不做日期过滤），用于全量备份。
func (a *ClickHouseAdapter) ListAllPartitions(c *shardConn, db, table string) ([]string, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("ListAllPartitions: nil conn for host %s", func() string {
			if c != nil {
				return c.host
			}
			return "<nil>"
		}())
	}
	query := fmt.Sprintf(
		"SELECT DISTINCT partition FROM system.parts WHERE database = '%s' AND `table` = '%s' AND active = 1 ORDER BY partition",
		strings.ReplaceAll(db, "'", "''"),
		strings.ReplaceAll(table, "'", "''"),
	)
	rows, err := c.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var partitions []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}

// realQueryTableDataPaths 从 system.tables 拿表的实际数据目录列表（Atomic 引擎下是
// store/<uuid>，传统引擎是 data/<db>/<table>，多 disk policy 可能多个 path）。
func (a *ClickHouseAdapter) realQueryTableDataPaths(c *shardConn, db, table string) ([]string, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("queryTableDataPaths: nil conn")
	}
	sql := fmt.Sprintf(
		"SELECT data_paths FROM system.tables WHERE database='%s' AND name='%s'",
		strings.ReplaceAll(db, "'", "''"),
		strings.ReplaceAll(table, "'", "''"),
	)
	rows, err := c.conn.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var paths []string
		if err := rows.Scan(&paths); err != nil {
			return nil, err
		}
		out = append(out, paths...)
	}
	return out, nil
}

// shellQuote 用单引号包裹路径，内部的 ' 转成 '\''，安全传给远端 shell。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// GetLastRunPartitions 从持久层查 365 天内同表的最近一次 success run 的 partitions。
func (a *ClickHouseAdapter) GetLastRunPartitions(cluster, db, table string) ([]model.BackupRunPartition, error) {
	runs, err := repository.Ps.GetRunsByTable(cluster, db, table, 365)
	if err != nil {
		return nil, err
	}
	for _, r := range runs {
		if r.Status == model.BACKUP_STATUS_SUCCESS {
			return r.Partitions, nil
		}
	}
	return nil, nil
}

// CollectChecksumOnHost 在 host 上通过 SSH 执行 md5sum，把结果填到 run.Partitions[i].PathInfo。
// 仅对 status=WAITING 的 partition 做。
func (a *ClickHouseAdapter) CollectChecksumOnHost(c *shardConn, run *model.BackupRun) error {
	cluster, err := a.getCluster(run.ClusterName)
	if err != nil {
		return err
	}
	opts := common.SshOptions{
		Host:             c.host,
		User:             cluster.SshUser,
		Password:         cluster.SshPassword,
		Port:             cluster.SshPort,
		NeedSudo:         cluster.NeedSudo,
		AuthenticateType: cluster.AuthenticateType,
	}
	// 数据路径从 CK 自身查，兼容 Atomic 引擎的 store/uuid 目录、自定义 path、多 disk。
	if a.queryDataPaths == nil {
		return fmt.Errorf("queryDataPaths hook not configured")
	}
	dataPaths, err := a.queryDataPaths(c, run.Database, run.Table)
	if err != nil {
		return fmt.Errorf("query data_paths for %s.%s: %w", run.Database, run.Table, err)
	}
	if len(dataPaths) == 0 {
		return fmt.Errorf("table %s.%s has no data_paths on host %s", run.Database, run.Table, c.host)
	}
	pathArgs := make([]string, len(dataPaths))
	for i, p := range dataPaths {
		pathArgs[i] = shellQuote(p)
	}
	for i := range run.Partitions {
		if run.Partitions[i].Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		// 用 -exec ... + 而不是 \; —— 后者经 ssh shell 解析后会变成裸 ; 让 find 报
		// "missing argument to -exec"。+ 批量传参，性能也更好。
		cmd := fmt.Sprintf("find %s -type f -exec md5sum {} +", strings.Join(pathArgs, " "))
		out, err := a.sshExec(opts, cmd)
		if err != nil {
			return fmt.Errorf("[%s] md5sum: %w", c.host, err)
		}
		if run.Partitions[i].PathInfo == nil {
			run.Partitions[i].PathInfo = make(map[string]model.PathInfo)
		}
		for _, line := range strings.Split(out, "\n") {
			f := strings.Fields(line)
			if len(f) != 2 {
				continue
			}
			run.Partitions[i].PathInfo[f[1]] = model.PathInfo{
				Host:  c.host,
				LPath: f[1],
				MD5:   f[0],
			}
		}
	}
	return nil
}
