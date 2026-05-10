package backup

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/backup/storage"
)

// NewStorageForPolicy：按 policy.TargetType 创建 BackupStorage 实现。
// Local 需要 cluster 信息构造 sshOpts；S3 不需要 cluster。
func NewStorageForPolicy(policy model.BackupPolicy, cluster model.CKManClickHouseConfig) BackupStorage {
	switch policy.TargetType {
	case model.BACKUP_S3:
		return storage.NewS3(policy.S3)
	case model.BACKUP_LOCAL:
		return storage.NewLocal(policy.Local, func(host string) common.SshOptions {
			return common.SshOptions{
				Host:             host,
				User:             cluster.SshUser,
				Password:         cluster.SshPassword,
				Port:             cluster.SshPort,
				NeedSudo:         cluster.NeedSudo,
				AuthenticateType: cluster.AuthenticateType,
			}
		})
	default:
		return nil
	}
}
