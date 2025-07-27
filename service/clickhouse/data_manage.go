package clickhouse

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

type Client struct {
	ckConns []*common.Conn
	sc      *S3Client
}

var client *Client

type Back struct {
	b    model.Backup
	c    Client
	conf model.CKManClickHouseConfig
	wg   sync.WaitGroup
	lock sync.Mutex
}

func (b *Back) SetStatus(status string) error {
	b.b.Status = status
	return repository.Ps.UpdateBackup(b.b)
}

func BackupManage(backupId string) error {
	backup, err := repository.Ps.GetBackupById(backupId)
	if err != nil {
		return err
	}
	b := Back{
		b: backup,
	}
	b.SetStatus(model.BACKUP_STATUS_INIT)
	log.Logger.Infof("[backup] init %s.%s", backup.Database, backup.Table)
	if err = b.Init(); err != nil {
		b.SetStatus(model.BACKUP_STATUS_FAILED)
		return err
	}
	log.Logger.Infof("[backup] %s.%s init success", backup.Database, backup.Table)

	if b.b.Operation == model.OP_BACKUP {
		b.SetStatus(model.BACKUP_STATUS_PREPARE)
		log.Logger.Infof("[backup]prepare %s.%s", backup.Database, backup.Table)
		if err = b.Prepare(); err != nil {
			b.SetStatus(model.BACKUP_STATUS_FAILED)
			return err
		}
		log.Logger.Infof("[backup] %s.%s prepare success", backup.Database, backup.Table)
		log.Logger.Infof("[backup] %s.%s backup", backup.Database, backup.Table)
		b.SetStatus(model.BACKUP_STATUS_BACKUP)
		if err = b.BackupData(); err != nil {
			b.SetStatus(model.BACKUP_STATUS_FAILED)
			return err
		}
		log.Logger.Infof("[backup] %s.%s backup success", backup.Database, backup.Table)
		log.Logger.Infof("[backup] %s.%s check", backup.Database, backup.Table)
		b.SetStatus(model.BACKUP_STATUS_CHECK)
		if err = b.Check(); err != nil {
			b.SetStatus(model.BACKUP_STATUS_FAILED)
			return err
		}
		log.Logger.Infof("[backup] %s.%s check success", backup.Database, backup.Table)
	} else if b.b.Operation == model.OP_RESTORE {
		log.Logger.Infof("[backup] %s.%s restore", backup.Database, backup.Table)
		b.SetStatus(model.BACKUP_STATUS_RESTORE)
		if err = b.RestoreData(); err != nil {
			b.SetStatus(model.BACKUP_STATUS_FAILED)
			return err
		}
		log.Logger.Infof("[backup] %s.%s restore success", backup.Database, backup.Table)
	} else {
		b.SetStatus(model.BACKUP_STATUS_FAILED)
		return errors.New("invalid operation")
	}
	b.SetStatus(model.BACKUP_STATUS_CLOSE)
	log.Logger.Infof("[backup] %s.%s close", backup.Database, backup.Table)
	if err = b.Close(); err != nil {
		b.SetStatus(model.BACKUP_STATUS_FAILED)
		return err
	}
	log.Logger.Infof("[backup] %s.%s done", backup.Database, backup.Table)
	b.SetStatus(model.BACKUP_STATUS_SUCCESS)
	return nil
}

func (b *Back) Init() error {
	// 连接ck（需重新指定max_read大小）
	log.Logger.Infof("[backup] connect to clickhouse")
	cluster, err := repository.Ps.GetClusterbyName(b.b.ClusterName)
	if err != nil {
		return err
	}
	b.conf = cluster
	opts := cluster.GetConnOption(
		model.WithMaxRead(21600),
	)
	for idx, shard := range cluster.Shards {
		found := false
		for _, replica := range shard.Replicas {
			conn, err := common.ConnectClickHouse(replica.Ip, model.ClickHouseDefaultDB, opts)
			if err == nil {
				b.c.ckConns = append(b.c.ckConns, conn)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("can not connect to shard %d", idx)
		}
	}

	if len(b.c.ckConns) == 0 {
		return errors.New("can not connect to clickhouse")
	}

	// 生成partition信息
	log.Logger.Infof("[backup] generate partition info")
	if b.b.DaysBefore > 0 {
		// 生成结束时间
		toPartition := time.Now().AddDate(0, 0, b.b.DaysBefore*(-1)).Format("20060102")
		// 获取表的所有分区，并按时间排序
		query := fmt.Sprintf("SELECT DISTINCT partition FROM cluster('%s', system.parts) WHERE partition <= '%s' AND database = '%s' AND table = '%s' ORDER BY partition",
			b.b.ClusterName, toPartition, b.b.Database, b.b.Table)
		rows, err := b.c.ckConns[0].Query(query)
		if err != nil {
			return err
		}
		defer rows.Close()
		var partitions []string
		for rows.Next() {
			var partition string
			if err = rows.Scan(&partition); err != nil {
				return err
			}
			partitions = append(partitions, partition)
		}
		// 从数据库中查询，排除已完成的分区
		binfo, err := repository.Ps.GetBackupByTable(b.b.ClusterName, b.b.Database, b.b.Table)
		if err == nil {
			// 生成本次需要备份的分区
			bpartitions := binfo.Partitions
			for _, partition := range partitions {
				found := false
				for _, p := range binfo.Partitions {
					if p.Partition == partition {
						found = true
						break
					}
				}
				if !found {
					bpartitions = append(bpartitions, model.BackupLists{
						//Hosts:     hosts,
						Partition: partition,
						//QueryId:   uuid.New(),
						Status: model.BACKUP_PARTITION_STATUS_WAITING,
					})
				}
			}
			b.b.Partitions = bpartitions
		}
	}

	for i := range b.b.Partitions {
		log.Logger.Debugf("[backup] table: %s.%s, partition: %s", b.b.Database, b.b.Table, b.b.Partitions[i].Partition)
		//b.b.Partitions[i].Hosts = hosts
	}

	err = repository.Ps.UpdateBackup(b.b)
	if err != nil {
		log.Logger.Errorf("[backup] update backup info failed: %v", err)
		return err
	}
	// 创建target client
	if b.b.TargetType == model.BACKUP_S3 {
		log.Logger.Infof("[backup] create s3 client")
		b.c.sc, err = NewS3Client(b.b.S3)
		if err != nil {
			return err
		}
		_ = b.c.sc.CreateBucket(b.b.S3.Bucket)
	} else if b.b.TargetType == model.BACKUP_LOCAL {
		// 检查是否开启了allowed_path
		log.Logger.Infof("[backup] check local path is allowed")
		query := fmt.Sprintf("BACKUP TABLE system.one TO File('%s/system.one/')", b.b.Local.Path)
		for _, conn := range b.c.ckConns {
			if err := conn.Exec(query); err != nil {
				return fmt.Errorf("code: 36, message: Path '%s' is not allowed for backups, see the 'backups.allowed_path' configuration parameter", b.b.Local.Path)
			}
		}
	}

	return nil
}

func (b *Back) Prepare() error {
	// 查询要备份的数据的行数，大小
	for i, partition := range b.b.Partitions {
		if partition.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		pexpr := fmt.Sprintf(" partition = '%s' AND ", partition.Partition)
		if partition.Partition == "all" {
			pexpr = ""
		}
		query := fmt.Sprintf("SELECT sum(bytes_on_disk), sum(rows) FROM cluster('%s', system.parts) WHERE %s database = '%s' AND table = '%s' and active = '1'", b.b.ClusterName, pexpr, b.b.Database, b.b.Table)
		log.Logger.Infof("query: %s", query)
		err := b.c.ckConns[0].QueryRow(query).Scan(&b.b.Partitions[i].Size, &b.b.Partitions[i].Rows)
		if err != nil {
			return err
		}
		log.Logger.Infof("partition: %s, size: %d, rows: %d", partition.Partition, b.b.Partitions[i].Size, b.b.Partitions[i].Rows)
	}
	repository.Ps.UpdateBackup(b.b)
	// 获取待备份的文件个数，md5sum
	for i, partition := range b.b.Partitions {
		if partition.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		fileNum := 0
		pexpr := fmt.Sprintf(" partition = '%s' AND ", partition.Partition)
		if partition.Partition == "all" {
			pexpr = ""
		}
		query := fmt.Sprintf("SELECT path FROM system.parts WHERE %s database = '%s' AND table = '%s' and active = '1'", pexpr, b.b.Database, b.b.Table)
		log.Logger.Infof("query: %s", query)
		b.wg.Add(len(b.c.ckConns))
		var lastErr error
		for _, conn := range b.c.ckConns {
			go func(conn *common.Conn) {
				defer b.wg.Done()
				rows, err := conn.Query(query)
				if err != nil {
					lastErr = err
					return
				}
				for rows.Next() {
					var path string
					if err = rows.Scan(&path); err != nil {
						lastErr = err
						return
					}
					opts := common.SshOptions{
						Host:             conn.Host(),
						User:             b.conf.SshUser,
						Password:         b.conf.SshPassword,
						Port:             b.conf.SshPort,
						NeedSudo:         b.conf.NeedSudo,
						AuthenticateType: b.conf.AuthenticateType,
					}
					// 获取文件的md5sum
					cmd := fmt.Sprintf("find %s -type f |xargs md5sum", path)
					out, err := common.RemoteExecute(opts, cmd)
					if err != nil {
						lastErr = err
					}
					log.Logger.Debugf("out: %s", out)
					for _, line := range strings.Split(out, "\n") {
						if line == "" {
							continue
						}
						fields := strings.Fields(line)
						if len(fields) != 2 {
							lastErr = fmt.Errorf("md5sum output format error: %s", line)
							return
						}
						md5sum := fields[0]
						pp := strings.Split(fields[1], "/")
						partfiles := strings.Join(pp[len(pp)-2:], "/")
						key := fmt.Sprintf("%s/%s.%s/%s/data/%s/%s/%s",
							partition.Partition, b.b.Database, b.b.Table, conn.Host(), b.b.Database, b.b.Table, partfiles)
						b.lock.Lock()
						if b.b.Partitions[i].PathInfo == nil {
							b.b.Partitions[i].PathInfo = make(map[string]model.PathInfo)
						}
						b.b.Partitions[i].PathInfo[key] = model.PathInfo{
							Host:  conn.Host(),
							RPath: key,
							LPath: fields[1],
							MD5:   md5sum,
						}
						fileNum++
						b.lock.Unlock()
						log.Logger.Debugf("clickhouse local path:[%s] path: %s, key: %s, checksum: %s", conn.Host(), fields[1], key, md5sum)
					}
				}
				rows.Close()
			}(conn)
		}
		b.wg.Wait()
		if lastErr != nil {
			return lastErr
		}
		b.b.Partitions[i].FileNum = uint64(fileNum)
	}
	repository.Ps.UpdateBackup(b.b)
	// 清理备份目录的原始文件
	if b.b.TargetType == model.BACKUP_LOCAL {
		for _, conn := range b.c.ckConns {
			opts := common.SshOptions{
				Host:             conn.Host(),
				User:             b.conf.SshUser,
				Password:         b.conf.SshPassword,
				Port:             b.conf.SshPort,
				NeedSudo:         b.conf.NeedSudo,
				AuthenticateType: b.conf.AuthenticateType,
			}
			//清理 system.one
			_, err := common.RemoteExecute(opts, fmt.Sprintf("rm -fr %s/system.one/", b.b.Local.Path))
			if err != nil {
				return err
			}
			// 清理对应partition
			for _, p := range b.b.Partitions {
				if p.Status != model.BACKUP_PARTITION_STATUS_WAITING {
					continue
				}
				_, err := common.RemoteExecute(opts, fmt.Sprintf("rm -fr %s/%s.%s/%s/", b.b.Local.Path, b.b.Database, b.b.Table, conn.Host()))
				if err != nil {
					return err
				}
			}
		}
	} else if b.b.TargetType == model.BACKUP_S3 {
		//清理对应partition
		for _, p := range b.b.Partitions {
			if p.Status != model.BACKUP_PARTITION_STATUS_WAITING {
				continue
			}
			for _, conn := range b.c.ckConns {
				key := fmt.Sprintf("%s/%s.%s/%s",
					p.Partition, b.b.Database, b.b.Table, conn.Host())
				err := b.c.sc.Remove(b.b.S3.Bucket, key)
				log.Logger.Debugf("remove %s from s3: %v", key, err)
			}
		}
	}
	return nil
}

func (b *Back) BackupData() error {
	//开始备份
	for i, partition := range b.b.Partitions {
		if partition.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		start := time.Now()
		b.b.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_RUNNING
		repository.Ps.UpdateBackup(b.b)
		// TODO：并发执行
		b.wg.Add(len(b.c.ckConns))
		for _, conn := range b.c.ckConns {
			go func(conn *common.Conn) {
				defer b.wg.Done()
				var key, sql string
				sql = fmt.Sprintf("BACKUP TABLE `%s`.`%s` ", b.b.Database, b.b.Table)
				if partition.Partition != "all" {
					sql += fmt.Sprintf(" PARTITION '%s'", partition.Partition)
				}
				key = fmt.Sprintf("%s/%s.%s/%s",
					partition.Partition, b.b.Database, b.b.Table, conn.Host())
				if b.b.TargetType == model.BACKUP_S3 {
					sql += fmt.Sprintf(" TO S3('%s/%s/%s', '%s', '%s')",
						b.b.S3.Endpoint, b.b.S3.Bucket, key, b.b.S3.AccessKeyID, b.b.S3.SecretAccessKey)
				} else if b.b.TargetType == model.BACKUP_LOCAL {
					sql += fmt.Sprintf(" TO File('%s/%s')", b.b.Local.Path, key)
				}

				sql += fmt.Sprintf(" SETTINGS compression_method='%s', compression_level=6, deduplicate_files = 0", b.b.Compression)
				log.Logger.Infof("query[%s]: %s", conn.Host(), sql)
				if err := conn.Exec(sql); err != nil {
					b.lock.Lock()
					b.b.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_FAILED
					b.b.Partitions[i].Msg = err.Error()
					b.b.Partitions[i].Elapsed = int(time.Since(start).Seconds())
					repository.Ps.UpdateBackup(b.b)
					log.Logger.Errorf("[%s](%s.%s)backup partition[%s] failed: %v", conn.Host(), b.b.Database, b.b.Table, partition.Partition, err)
					b.lock.Unlock()
					return
				}
				b.lock.Lock()
				b.b.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_SUCCESS
				b.b.Partitions[i].Elapsed = int(time.Since(start).Seconds())
				repository.Ps.UpdateBackup(b.b)
				b.lock.Unlock()
			}(conn)
		}
		b.wg.Wait()
	}
	return nil
}

func (b *Back) RestoreData() error {
	//开始恢复
	for i, partition := range b.b.Partitions {
		if partition.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		start := time.Now()
		b.b.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_RUNNING
		// TODO：并发执行
		b.wg.Add(len(b.c.ckConns))
		for _, conn := range b.c.ckConns {
			go func(conn *common.Conn) {
				defer b.wg.Done()
				var key, sql string
				sql = fmt.Sprintf("RESTORE TABLE `%s`.`%s` ", b.b.Database, b.b.Table)
				if partition.Partition != "all" {
					sql += fmt.Sprintf(" PARTITION '%s'", partition.Partition)
				}
				key = fmt.Sprintf("%s/%s.%s/%s",
					partition.Partition, b.b.Database, b.b.Table, conn.Host())
				if b.b.TargetType == model.BACKUP_S3 {
					sql += fmt.Sprintf(" FROM S3('%s/%s/%s', '%s', '%s')",
						b.b.S3.Endpoint, b.b.S3.Bucket, key, b.b.S3.AccessKeyID, b.b.S3.SecretAccessKey)
				} else if b.b.TargetType == model.BACKUP_LOCAL {
					sql += fmt.Sprintf(" FROM File('%s/%s')", b.b.Local.Path, key)
				}

				sql += " SETTINGS allow_non_empty_tables=true"
				log.Logger.Infof("[%s] restore partition %s", conn.Host(), partition.Partition)
				log.Logger.Infof("[%s] query: %s", conn.Host(), sql)
				if err := conn.Exec(sql); err != nil {
					b.lock.Lock()
					b.b.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_FAILED
					b.b.Partitions[i].Msg = err.Error()
					b.b.Partitions[i].Elapsed = int(time.Since(start).Seconds())
					repository.Ps.UpdateBackup(b.b)
					log.Logger.Errorf("[%s](%s.%s)restore partition[%s] failed: %v", conn.Host(), b.b.Database, b.b.Table, partition.Partition, err)
					b.lock.Unlock()
					return
				}
				b.lock.Lock()
				b.b.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_SUCCESS
				b.b.Partitions[i].Elapsed = int(time.Since(start).Seconds())
				repository.Ps.UpdateBackup(b.b)
				b.lock.Unlock()
			}(conn)
		}
		b.wg.Wait()
	}
	return nil
}

func (b *Back) Check() error {
	//检查备份后的文件个数，md5sum
	for _, partition := range b.b.Partitions {
		if partition.Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
			continue
		}
		b.wg.Add(len(b.c.ckConns))
		var lastErr error
		for _, conn := range b.c.ckConns {
			go func(conn *common.Conn) {
				defer b.wg.Done()
				if b.b.TargetType == model.BACKUP_S3 {
					key := fmt.Sprintf("%s/%s.%s/%s",
						partition.Partition, b.b.Database, b.b.Table, conn.Host())
					_, _, _, err := b.c.sc.CheckSum(conn.Host(), b.b.S3.Bucket, key, partition.PathInfo, b.b.S3)
					if err != nil {
						lastErr = err
						return
					}
				} else if b.b.TargetType == model.BACKUP_LOCAL {
					// todo
					log.Logger.Infof("check local path: %s", partition.Partition)
				}
			}(conn)
		}
		b.wg.Wait()
		return lastErr
	}
	return nil
}

func (b *Back) Close() error {
	// 清理原始数据库的数据
	if b.b.Clean {
		for _, partition := range b.b.Partitions {
			if partition.Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
				continue
			}
			for _, conn := range b.c.ckConns {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PARTITION '%s'", b.b.Database, b.b.Table, partition.Partition)
				log.Logger.Infof("host %s: query: %s", conn.Host(), query)
				_ = conn.Exec(query)
			}
		}
	}

	// // 关闭client
	// for _, conn := range b.c.ckConns {
	// 	conn.Close()
	// }
	// if b.b.TargetType == model.BACKUP_S3 {
	// 	b.c.sc = nil
	// }
	return nil
}
