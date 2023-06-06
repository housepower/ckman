package clickhouse

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Factory struct{}

func (S3Factory) Create() Archive {
	return &TargetS3{}
}

type TargetS3 struct {
	ArchiveParams
	s3  model.ArchiveS3
	svc *s3.S3
}

func (t *TargetS3) Normalize(params ArchiveParams, req model.ArchiveTableReq) error {
	t.ArchiveParams = params
	t.s3 = req.S3
	if t.s3.Endpoint == "" {
		t.s3.Endpoint = "localhost"
	}
	if t.s3.Compression == "" {
		t.s3.Compression = "gzip"
	}

	if t.s3.Compression != "none" {
		t.Suffix += "." + t.s3.Compression
	}

	return nil
}

func (t *TargetS3) Init() error {
	if err := t.InitConns(); err != nil {
		return err
	}
	if err := t.GetSortingInfo(); err != nil {
		return err
	}
	sc, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(t.s3.AccessKeyID, t.s3.SecretAccessKey, ""),
		Endpoint:         aws.String(t.s3.Endpoint),
		Region:           aws.String(t.s3.Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return err
	}
	t.svc = s3.New(sc)

	return nil
}

func (t *TargetS3) Engine(fp string) string {
	var access string
	if t.s3.AccessKeyID != "" {
		access += ", '" + t.s3.AccessKeyID + "'"
	}
	if t.s3.SecretAccessKey != "" {
		access += ", '" + t.s3.SecretAccessKey + "'"
	}
	uri, _ := url.JoinPath(t.s3.Endpoint, fp)
	return fmt.Sprintf("S3('%s'%s, '%s', '%s')", uri, access, t.Format, t.s3.Compression)
}

func (t *TargetS3) Clear() error {
	slotBeg, _ := time.Parse(DateLayout, t.Begin)
	slotEnd, _ := time.Parse(DateLayout, t.End)
	params := &s3.CreateBucketInput{
		Bucket: aws.String(t.s3.Bucket),
	}

	_, err := t.svc.CreateBucket(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
			case s3.ErrCodeBucketAlreadyOwnedByYou:
			default:
				log.Logger.Errorf("bucket %s not exists:%v", t.s3.Bucket, aerr)
				return aerr
			}
		} else {
			log.Logger.Errorf("bucket %s not exists:%v", t.s3.Bucket, err)
			return err
		}
	}
	for _, table := range t.Tables {
		dir := path.Join(t.s3.Bucket, t.Cluster, t.Database+"."+table)
		t.Dirs = append(t.Dirs, dir)
		params := &s3.ListObjectsInput{
			Bucket: aws.String(t.s3.Bucket),
		}
		resp, err := t.svc.ListObjects(params)
		if err != nil {
			return err
		}
		for _, item := range resp.Contents {
			name := path.Base(*item.Key)
			if dir != path.Dir(*item.Key) {
				continue
			}
			splits := strings.Split(name, "_")
			if len(splits) < 3 {
				continue
			}
			slot := splits[3] //shard_%d_host_slot.suffix
			idx := strings.Index(slot, ".")
			slot = slot[:idx]
			slotTime, err := time.Parse(SlotTimeFormat, slot)
			if err != nil {
				log.Logger.Errorf("parse time error: %+v", err)
				return err
			}
			if (slotTime.After(slotBeg) || slotTime.Equal(slotBeg)) && slotTime.Before(slotEnd) {
				t.svc.DeleteObject(&s3.DeleteObjectInput{
					Bucket: aws.String(t.s3.Bucket),
					Key:    item.Key,
				})
			}
		}
	}

	return nil
}

func (t *TargetS3) Export() error {
	var err error
	if err = t.GetAllSlots(); err != nil {
		return err
	}
	t0 := time.Now()
	var lastErr error
	var wg sync.WaitGroup
	for i, host := range t.Hosts {
		wg.Add(1)
		i := i
		host := host
		go func() {
			defer wg.Done()
			for _, slot := range t.Slots {
				if slot.Host != host {
					continue
				}
				engines := make([]string, 0)
				for _, dir := range t.Dirs {
					fp := filepath.Join(dir, fmt.Sprintf("shard_%d_%s", i, host), fmt.Sprintf("archive_%s_%v", slot.Table, slot.SlotBeg.Format(SlotTimeFormat)), "data"+t.Suffix)
					engines = append(engines, t.Engine(fp))
				}
				if err = t.ExportSlot(host, slot.Table, i, slot.SlotBeg, slot.SlotEnd, engines); err != nil {
					lastErr = err
				}
			}
		}()
	}
	wg.Wait()

	if lastErr != nil {
		return err
	}

	du := uint64(time.Since(t0).Seconds())
	size := atomic.LoadUint64(&t.EstSize)
	msg := fmt.Sprintf("exported %d bytes in %d seconds", size, du)
	if du != 0 {
		msg += fmt.Sprintf(", %d bytes/s", size/du)
	}
	log.Logger.Infof(msg)
	return nil
}

func (t *TargetS3) Done(_ string) {
	for _, host := range t.Hosts {
		conn := common.GetConnection(host)
		for _, tmpTbl := range t.TmpTables {
			query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTbl)
			conn.Exec(context.Background(), query)
		}
	}
}
