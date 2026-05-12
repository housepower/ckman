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

func (s *S3) CleanPartition(database, table, host, partition string) error {
	if s.client == nil {
		return errors.New("s3 not initialized")
	}
	key := fmt.Sprintf("%s/%s.%s/%s", partition, database, table, host)
	return s.client.Remove(s.cfg.Bucket, key)
}

func (s *S3) CheckPartition(host, database, table, partition string, pathInfo map[string]model.PathInfo) error {
	if s.client == nil {
		return errors.New("s3 not initialized")
	}
	key := fmt.Sprintf("%s/%s.%s/%s", partition, database, table, host)
	_, _, _, err := s.client.CheckSum(host, s.cfg.Bucket, key, pathInfo, s.cfg)
	return err
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
