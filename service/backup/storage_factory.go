package backup

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/backup/storage"
)

// NewStorageForPolicy：按 policy.TargetType 创建 BackupStorage 实现。
// Local 需要 cluster 信息构造 sshOpts；S3 不需要 cluster。
// policy.Compression 透传给 storage，让 BackupSQL 生成 SETTINGS compression_method=...。
func NewStorageForPolicy(policy model.BackupPolicy, cluster model.CKManClickHouseConfig) BackupStorage {
	switch policy.TargetType {
	case model.BACKUP_S3:
		return storage.NewS3(policy.S3, policy.Compression)
	case model.BACKUP_LOCAL:
		return storage.NewLocal(policy.Local, policy.Compression, func(host string) common.SshOptions {
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
