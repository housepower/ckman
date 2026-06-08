package backup

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/backup/storage"
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
// getCluster / dial / sshExec / queryPartitionPaths 是注入点，便于单测；NewClickHouseAdapter 用真实实现。
type ClickHouseAdapter struct {
	getCluster          func(string) (model.CKManClickHouseConfig, error)
	dial                func(host string, opt model.ConnetOption) (*common.Conn, error)
	sshExec             func(opts common.SshOptions, cmd string) (string, error)
	queryPartitionPaths func(c *shardConn, db, table, partition string) ([]string, error)
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
	a.queryPartitionPaths = a.realQueryPartitionPaths
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

// ListPartitions 列指定表 fromYYYYMMDD <= partition <= toYYYYMMDD 的所有分区。
// fromYYYYMMDD 为空时不限制下界，兼容老的「<= N 天前」策略。
func (a *ClickHouseAdapter) ListPartitions(c *shardConn, db, table, fromYYYYMMDD, toYYYYMMDD string) ([]string, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("ListPartitions: nil conn for host %s", func() string {
			if c != nil {
				return c.host
			}
			return "<nil>"
		}())
	}
	where := fmt.Sprintf(
		"partition <= '%s' AND database = '%s' AND `table` = '%s' AND active = 1",
		strings.ReplaceAll(toYYYYMMDD, "'", "''"),
		strings.ReplaceAll(db, "'", "''"),
		strings.ReplaceAll(table, "'", "''"),
	)
	if fromYYYYMMDD != "" {
		where = fmt.Sprintf("partition >= '%s' AND %s", strings.ReplaceAll(fromYYYYMMDD, "'", "''"), where)
	}
	query := fmt.Sprintf(
		"SELECT DISTINCT partition FROM system.parts WHERE %s ORDER BY partition",
		where,
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

// realQueryPartitionPaths 拿 partition 在本 shard 上每个 active part 的目录（system.parts.path）。
// 返回如 [/data01/clickhouse/store/abc/xxx/20260304_1_5_2, ...]，每个目录对应一个 active part。
// 路径来源是 CK 自己报告的，自动兼容 Atomic engine / 自定义 path / 多 disk policy。
func (a *ClickHouseAdapter) realQueryPartitionPaths(c *shardConn, db, table, partition string) ([]string, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("queryPartitionPaths: nil conn")
	}
	var where string
	if partition != "" && partition != "all" {
		where = fmt.Sprintf("partition='%s' AND ", strings.ReplaceAll(partition, "'", "''"))
	}
	sql := fmt.Sprintf(
		"SELECT path FROM system.parts WHERE %sdatabase='%s' AND `table`='%s' AND active=1",
		where,
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
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		// 去掉末尾斜杠，保证拼接 part_dir/file 时不出双斜杠
		p = strings.TrimRight(p, "/")
		out = append(out, p)
	}
	return out, nil
}

// shellQuote 用单引号包裹路径，内部的 ' 转成 '\”，安全传给远端 shell。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// GetLastRunPartitions 从持久层查全部历史同表已成功备份的 partitions，
// 调用方用它做增量去重。判定是分区级而非 run 级（见 successfulPartitionsFromRuns）；
// 不能只取最近一次 run，否则当前 run 不再记录历史 success 后，下一轮会丢失
// 更早的去重历史。
// 若窗口有限（如原 365 天），备份记录滚出窗口后老分区会被定时备份周期性重复备份，
// 故取全部历史（sinceDays=0）：删记录成为唯一让分区重新备份的开关。
//
// 开销说明：全历史扫描会反序列化该表所有历史 run 的 Partitions 字段；
// 对运行数年、每天备份的表，run 数量可达上千条，内存与 CPU 开销随历史深度线性增长。
// 这是为消除"老分区记录滚出窗口被周期性重备"缺陷而做的有意取舍——
// DeletePartitionRecords 是唯一让分区重新备份的手段。
func (a *ClickHouseAdapter) GetLastRunPartitions(cluster, db, table string) ([]model.BackupRunPartition, error) {
	runs, err := repository.Ps.GetRunsByTable(cluster, db, table, 0)
	if err != nil {
		return nil, err
	}
	return successfulPartitionsFromRuns(runs), nil
}

// successfulPartitionsFromRuns 从 runs（须按时间新→旧排序，GetRunsByTable 保证）
// 提取"最近一次执行结果为 success"的分区。按分区粒度判定而非 run 粒度：
// run 整体 failed 时其中已 success 的分区仍计入去重，避免 100 个分区 1 个失败
// 导致下一轮整表重备——重备本身又可能出现新的失败分区，失败范围反而蔓延。
// 每个分区以最近一次终态（success/failed）记录定型：更早的 success 不能掩盖
// 最近的 failed，因为失败的重备可能已破坏存储上同 key 的旧数据。
// restore run 不参与备份去重；waiting/running 分区尚无结果，不定型。
func successfulPartitionsFromRuns(runs []model.BackupRun) []model.BackupRunPartition {
	seen := make(map[string]bool)
	partitions := make([]model.BackupRunPartition, 0)
	for _, r := range runs {
		// 只排除明确的 restore；老版本迁移来的 run Operation 可能为空，视为 backup
		if r.Operation == model.OP_RESTORE {
			continue
		}
		for _, p := range r.Partitions {
			if p.Status != model.BACKUP_PARTITION_STATUS_SUCCESS && p.Status != model.BACKUP_PARTITION_STATUS_FAILED {
				continue
			}
			if seen[p.Partition] {
				continue
			}
			seen[p.Partition] = true
			if p.Status == model.BACKUP_PARTITION_STATUS_SUCCESS {
				partitions = append(partitions, p)
			}
		}
	}
	return partitions
}

// CollectChecksumOnHost 在 host 上通过 SSH 执行 md5sum，把结果填到 run.Partitions[i].PathInfo。
// 仅对 status=WAITING 的 partition 做。
//
// 路径关联（与 ClickHouse 备份到 storage 的 key 命名对齐）：
//
//	storage key = [<storage_prefix>/]<partition>/<db>.<table>/<host>/data/<db>/<table>/<part_dir>/<file>
//	local file  = <data_path>/<part_dir>/<file>（CK 报告的 system.parts.path 末段）
//
// 因此从 system.parts.path 拿到的本地路径取末两段 (<part_dir>/<file>) 拼到 JoinRunKey
// 返回值之后即可得到对应 object 的 RPath，CheckSum 阶段 ListObjects(Prefix=path.Dir(RPath))
// 即能精确命中。
func (a *ClickHouseAdapter) CollectChecksumOnHost(c *shardConn, run *model.BackupRun) error {
	cluster, err := a.getCluster(run.ClusterName)
	if err != nil {
		return err
	}
	if a.queryPartitionPaths == nil {
		return fmt.Errorf("queryPartitionPaths hook not configured")
	}
	opts := common.SshOptions{
		Host:             c.host,
		User:             cluster.SshUser,
		Password:         cluster.SshPassword,
		Port:             cluster.SshPort,
		NeedSudo:         cluster.NeedSudo,
		AuthenticateType: cluster.AuthenticateType,
	}
	for i := range run.Partitions {
		if run.Partitions[i].Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		partName := run.Partitions[i].Partition
		// 拿这个 partition 在本 shard 上的所有 part 目录
		partPaths, err := a.queryPartitionPaths(c, run.Database, run.Table, partName)
		if err != nil {
			return fmt.Errorf("query partition paths for %s.%s partition=%s: %w",
				run.Database, run.Table, partName, err)
		}
		if len(partPaths) == 0 {
			continue // 这台 shard 上没有该 partition 的数据
		}
		if run.Partitions[i].PathInfo == nil {
			run.Partitions[i].PathInfo = make(map[string]model.PathInfo)
		}
		fileNum := 0
		for _, pp := range partPaths {
			cmd := fmt.Sprintf("find %s -type f -exec md5sum {} +", shellQuote(pp))
			out, err := a.sshExec(opts, cmd)
			if err != nil {
				return fmt.Errorf("[%s] md5sum %s: %w", c.host, pp, err)
			}
			for _, line := range strings.Split(out, "\n") {
				f := strings.Fields(line)
				if len(f) != 2 {
					continue
				}
				md5sum := f[0]
				localPath := f[1]
				// 取末两段 (<part_dir>/<file>) 拼成 S3 上对应 object 的 key 末尾
				segs := strings.Split(localPath, "/")
				if len(segs) < 2 {
					continue
				}
				partfile := strings.Join(segs[len(segs)-2:], "/")
				keyPrefix := storage.JoinRunKey(run.StoragePrefix, partName, run.Database, run.Table, c.host)
				rpath := fmt.Sprintf("%s/data/%s/%s/%s", keyPrefix, run.Database, run.Table, partfile)
				run.Partitions[i].PathInfo[rpath] = model.PathInfo{
					Host:  c.host,
					RPath: rpath,
					LPath: localPath,
					MD5:   md5sum,
				}
				fileNum++
			}
		}
		run.Partitions[i].FileNum = uint64(fileNum)
	}
	return nil
}
