package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housepower/ckman/model"
)

type S3 struct {
	cfg         model.TargetS3
	compression string
	client      *S3Client
}

func NewS3(cfg model.TargetS3, compression string) *S3 {
	// trim 末尾斜杠，避免拼出 host//bucket 让 ClickHouse 解析失败
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")
	return &S3{cfg: cfg, compression: compression}
}

func (s *S3) Init() error {
	if !strings.HasPrefix(s.cfg.Endpoint, "http://") && !strings.HasPrefix(s.cfg.Endpoint, "https://") {
		return errors.New("s3 endpoint must start with http:// or https://")
	}
	c, err := NewS3Client(s.cfg)
	if err != nil {
		return err
	}
	s.client = c
	_ = c.CreateBucket(s.cfg.Bucket) // 已存在不报错
	return nil
}

func (s *S3) Type() string { return model.BACKUP_S3 }

func (s *S3) BackupSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", escapeSQLLiteral(partition)))
	}
	sb.WriteString(fmt.Sprintf(" TO S3('%s/%s/%s', '%s', '%s')",
		s.cfg.Endpoint, s.cfg.Bucket, key,
		escapeSQLLiteral(s.cfg.AccessKeyID),
		escapeSQLLiteral(s.cfg.SecretAccessKey)))
	sb.WriteString(backupSettings(s.compression))
	return sb.String()
}

func (s *S3) RestoreSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", escapeSQLLiteral(partition)))
	}
	sb.WriteString(fmt.Sprintf(" FROM S3('%s/%s/%s', '%s', '%s')",
		s.cfg.Endpoint, s.cfg.Bucket, key,
		escapeSQLLiteral(s.cfg.AccessKeyID),
		escapeSQLLiteral(s.cfg.SecretAccessKey)))
	sb.WriteString(restoreSettings())
	return sb.String()
}

func (s *S3) CleanPartition(host, keyPrefix string) error {
	_ = host // S3 是 server-side 删除，host 仅 Local 后端使用
	if s.client == nil {
		return errors.New("s3 not initialized")
	}
	return s.client.Remove(s.cfg.Bucket, keyPrefix)
}

func (s *S3) CheckPartition(host, keyPrefix string, pathInfo map[string]model.PathInfo) error {
	if s.client == nil {
		return errors.New("s3 not initialized")
	}
	// 仅当备份时未压缩，md5(local raw) ↔ md5(S3 object) 才会相等；
	// 压缩开启时只能校验文件存在性与数量。
	compareMD5 := s.compression == "" || strings.EqualFold(s.compression, "none")
	_, _, _, err := s.client.CheckSum(host, s.cfg.Bucket, keyPrefix, pathInfo, s.cfg, compareMD5)
	return err
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
