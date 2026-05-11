package backup

import (
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

// DiskInfo 是 BackupService 暴露给前端的 disk 元数据，仅包含 local 类型可用作备份目标的 disk。
type DiskInfo struct {
	Name          string `json:"name"`
	Type          string `json:"type"`           // 现阶段只返回 "local"
	Path          string `json:"path"`           // local disk 的 path
	AllowedBackup bool   `json:"allowed_backup"` // ckman 配置中是否标了 allow_backup
}

// filterLocalDisks 是核心纯函数：从 CKManClickHouseConfig 中提取所有 type=local 且 DiskLocal!=nil 的 disk。
// cluster.Storage==nil（用户手动改 ClickHouse XML 没走 ckman）→ 返回空数组，前端按风险提示处理。
func filterLocalDisks(cc model.CKManClickHouseConfig) []DiskInfo {
	out := []DiskInfo{}
	if cc.Storage == nil {
		return out
	}
	for _, d := range cc.Storage.Disks {
		if d.Type != "local" || d.DiskLocal == nil {
			continue
		}
		out = append(out, DiskInfo{
			Name:          d.Name,
			Type:          d.Type,
			Path:          d.DiskLocal.Path,
			AllowedBackup: d.AllowedBackup,
		})
	}
	return out
}

// GetClusterDisks 返回 cluster 配置中所有 type=local 的 disk 列表。
// allowed_backup 字段直接从 ckman 自己的 Storage 配置读，不查 ClickHouse system.disks。
// 如果 cluster 没配置 Storage（用户手动改 ClickHouse XML 没走 ckman），返回空数组，前端按风险提示处理。
func (s *Service) GetClusterDisks(cluster string) ([]DiskInfo, error) {
	cc, err := repository.Ps.GetClusterbyName(cluster)
	if err != nil {
		return nil, err
	}
	return filterLocalDisks(cc), nil
}
