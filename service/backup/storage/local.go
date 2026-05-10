package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/backup/bvalidate"
)

// Local 实现 Storage 接口，将备份文件写到 ClickHouse 节点的本地文件系统。
// sshOpts 以依赖注入的方式提供：调用方（持有 cluster 配置的层）负责构造并传入，
// 让 storage 包无需直接依赖 cluster 模型。
type Local struct {
	cfg     model.TargetLocal
	sshOpts func(host string) common.SshOptions // 可为 nil；nil 时远程操作返回错误
}

// NewLocal 构造 Local storage 实例。
// sshOpts 传 nil 时，Init / BackupSQL / RestoreSQL 仍可使用，
// 但 CleanPartition / CheckPartition 会返回错误。
func NewLocal(cfg model.TargetLocal, sshOpts func(string) common.SshOptions) *Local {
	return &Local{cfg: cfg, sshOpts: sshOpts}
}

// Init 对配置路径执行白名单校验（修闭 spec §9 #9 RCE 注入面）。
func (l *Local) Init() error {
	return bvalidate.ValidateLocalPath(l.cfg.Path)
}

// Type 返回存储类型标识。
func (l *Local) Type() string { return model.BACKUP_LOCAL }

// BackupSQL 返回 BACKUP TABLE … 语句的尾部子句（PARTITION + TO File(…)）。
// partition="all" 时省略 PARTITION 子句。
func (l *Local) BackupSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", strings.ReplaceAll(partition, "'", "''")))
	}
	sb.WriteString(fmt.Sprintf(" TO File('%s/%s')", l.cfg.Path, key))
	return sb.String()
}

// RestoreSQL 返回 RESTORE TABLE … 语句的尾部子句（PARTITION + FROM File(…)）。
func (l *Local) RestoreSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", strings.ReplaceAll(partition, "'", "''")))
	}
	sb.WriteString(fmt.Sprintf(" FROM File('%s/%s')", l.cfg.Path, key))
	return sb.String()
}

// CleanPartition 通过 SSH 删除目标节点上该 partition 的备份目录。
// database / table / partition 均经 ValidateIdentifier 强校验，防 shell 注入。
func (l *Local) CleanPartition(database, table, host, partition string) error {
	if l.sshOpts == nil {
		return errors.New("local storage: sshOpts not configured")
	}
	// identifier 强校验，防 shell 注入（双层防御：校验 + %q 引用）
	for _, s := range []string{database, table, partition} {
		if err := bvalidate.ValidateIdentifier(s); err != nil {
			return err
		}
	}
	opts := l.sshOpts(host)
	// %q 对路径进行 Go 风格引用（bash 同样接受双引号路径），防止路径中意外空格等
	cmd := fmt.Sprintf("rm -fr %q/%s.%s/%s/", l.cfg.Path, database, table, host)
	_, err := common.RemoteExecute(opts, cmd)
	return err
}

// CheckPartition 通过 SSH 在 ClickHouse 节点上执行 md5sum，并与 pathInfo 中预期值比对。
// 修闭老版 "// todo" 衍生问题：本地备份开 checksum 时实际不做校验仍标 success。
func (l *Local) CheckPartition(host, database, table, partition string,
	pathInfo map[string]model.PathInfo) error {
	if l.sshOpts == nil {
		return errors.New("local storage: sshOpts not configured")
	}
	if err := bvalidate.ValidateIdentifier(database); err != nil {
		return err
	}
	if err := bvalidate.ValidateIdentifier(table); err != nil {
		return err
	}
	opts := l.sshOpts(host)
	root := fmt.Sprintf("%s/%s.%s/%s", l.cfg.Path, database, table, host)
	cmd := fmt.Sprintf("find %q -type f -exec md5sum {} \\;", root)
	out, err := common.RemoteExecute(opts, cmd)
	if err != nil {
		return err
	}

	// 解析 md5sum 输出：每行格式为 "<hash>  <path>"
	gotByPath := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		gotByPath[f[1]] = f[0]
	}

	// 仅校验 host 匹配的条目，与 pathInfo 中预期 MD5 逐一比对
	for _, pi := range pathInfo {
		if pi.Host != host {
			continue
		}
		got, ok := gotByPath[pi.LPath]
		if !ok {
			return fmt.Errorf("local checksum: path missing %s on %s", pi.LPath, host)
		}
		if got != pi.MD5 {
			return fmt.Errorf("local checksum mismatch: %s on %s (got=%s expected=%s)",
				pi.LPath, host, got, pi.MD5)
		}
	}
	return nil
}
