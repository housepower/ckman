package clickhouse

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

type S3Client struct {
	svc  *s3.S3
	conf model.TargetS3
}

func NewS3Client(conf model.TargetS3) (*S3Client, error) {
	var endpoint string
	var sc *session.Session
	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return nil, err
	}
	if u.Path != "" {
		endpoint = u.Scheme + "://" + u.Host
		conf.Bucket = strings.Split(u.Path, "/")[1]
	} else {
		endpoint = conf.Endpoint
	}
	log.Logger.Infof("path: %s, endpoint: %s, bucket: %s", u.Path, endpoint, conf.Bucket)

	httpClient := &http.Client{}
	if conf.UseSSL && conf.CAFile != "" {
		caCert, err := os.ReadFile(conf.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}
		if conf.InsecureSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(conf.AccessKeyID, conf.SecretAccessKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(conf.Region),
		DisableSSL:       aws.Bool(!conf.UseSSL), // 支持启用/禁用 SSL
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient, // 使用自定义 HTTP 客户端
	}
	sc, err = session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}
	svc := s3.New(sc)
	return &S3Client{
		svc:  svc,
		conf: conf,
	}, nil
}

func (c *S3Client) CreateBucket(bucket string) error {
	params := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := c.svc.CreateBucket(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
			case s3.ErrCodeBucketAlreadyOwnedByYou:
			default:
				log.Logger.Errorf("bucket %s not exists:%v", bucket, aerr)
				return aerr
			}
		} else {
			log.Logger.Errorf("bucket %s not exists:%v", bucket, err)
			return err
		}
	}
	return nil
}

func (c *S3Client) Remove(bucket, key string) error {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}
	resp, err := c.svc.ListObjects(params)
	if err != nil {
		return err
	}
LOOP_DEL:
	log.Logger.Infof("key %s has %d objects need to delete", key, len(resp.Contents))
	for _, item := range resp.Contents {
		log.Logger.Debugf("%s need to delete", *item.Key)
		_, err := c.svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    item.Key,
		})
		if err != nil {
			log.Logger.Errorf("delete %s failed", *item.Key)
			return err
		} else {
			err = c.svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    item.Key,
			})
			if err != nil {
				log.Logger.Errorf("delete %s failed", *item.Key)
				return err
			}
			log.Logger.Debugf("%s deleted", *item.Key)
		}
	}

	//由于一次最多只能删除1000个object，删除之后查一把还有没有没删除的
	resp, err = c.svc.ListObjects(params)
	if err != nil {
		return err
	}
	if len(resp.Contents) == 0 {
		log.Logger.Infof("object %s is empty", key)
	} else {
		//没删除干净，再来一次
		log.Logger.Infof("object %s is not empty, still %d objects remained, try again", key, len(resp.Contents))
		goto LOOP_DEL
	}
	return nil
}

func (c *S3Client) CheckSum(host string, bucket, key string, paths map[string]model.PathInfo, conf model.TargetS3) (map[string]model.PathInfo, uint64, int, error) {
	var rsize uint64
	errPaths := make(map[string]model.PathInfo)

	subKeys := make(map[string]struct{})
	for _, v := range paths {
		if v.Host != host {
			continue
		}
		subKey := path.Dir(v.RPath)
		if _, ok := subKeys[subKey]; ok {
			continue
		}
		subKeys[subKey] = struct{}{}
	}

	rpaths := make(map[string]string)
	rcnts := make(map[string]int)
	cnt := 0
	for subkey := range subKeys {
		params := &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(subkey),
		}
		resp, err := c.svc.ListObjects(params)
		if err != nil {
			return errPaths, rsize, -1, err
		}

		subCnt := 0
		for _, item := range resp.Contents {
			checksum := strings.Trim(*item.ETag, "\"")
			if strings.Contains(checksum, "-") {
				//分段上传, 由于不知道UploadId, 无法计算具体的MD5值, 需要将对象下载下来，分段计算MD5
				log.Logger.Infof("key %s is multipart upload, checksum: %s", *item.Key, checksum)
				output, err := c.svc.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    item.Key,
				})
				if err != nil {
					return errPaths, rsize, -1, err
				}
				defer output.Body.Close()
				//一次读取32MB
				segment := make([]byte, 1048576*32)
				hash := md5.New()
				for {
					n, err := output.Body.Read(segment)
					if err != nil && err != io.EOF {
						return errPaths, rsize, -1, err
					}
					if n == 0 {
						break
					}
					hash.Write(segment[:n])
				}
				checksum = hex.EncodeToString(hash.Sum(nil))
			}
			size := *item.Size
			rsize += uint64(size)
			subCnt++
			rpaths[*item.Key] = checksum
			log.Logger.Debugf("[%s]remote s3 path: %s, checksum: %s", host, *item.Key, checksum)
		}
		log.Logger.Infof("[%s] %s remote count: %d", host, subkey, subCnt)
		cnt += subCnt
		rcnts[subkey+"/"] = subCnt
		if subCnt == 1000 {
			log.Logger.Warnf("NOTICE: %s has more than 1000 keys, may not list all.", subkey)
		}
	}
	log.Logger.Infof("[%s] %s remote total count: %d", host, key, cnt)
	var err error
	for k, v := range paths {
		if v.Host != host {
			continue
		}
		if checksum, ok := rpaths[k]; ok {
			if v.MD5 != checksum {
				errPaths[k] = v
				err = fmt.Errorf("checksum mismatch for %v, expect %v, but got %v", k, v.MD5, checksum)
				log.Logger.Warnf("[%s]%v", host, err)
			}

		} else {
			if cnt, ok := rcnts[k]; ok {
				if cnt != v.Cnt {
					errPaths[k] = v
					err = fmt.Errorf("check count mismatch for %v, expect %v, but got %v", k, v.Cnt, cnt)
					log.Logger.Warnf("[%s]%v", host, err)
				}
			}
		}
	}
	log.Logger.Infof("errPaths: %d", len(errPaths))
	return errPaths, rsize, cnt, err
}

func (c *S3Client) Upload(bucket, folderPath, key string, dryrun bool) error {
	pool := common.NewWorkerPool(common.MaxWorkersDefault, common.MaxWorkersDefault*2)
	var lastErr error
	defer pool.Close()
	cnt := 0
	start := time.Now()
	err := filepath.Walk(folderPath, func(fpath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Logger.Errorf("Error walking the path:%v", err)
			return err
		}

		log.Logger.Debugf("Visiting:%v", fpath)

		if !info.IsDir() {
			pool.Submit(func() {
				if err := c.uploadFile(key, fpath, bucket, dryrun); err != nil {
					lastErr = err
					return
				}
				cnt++
			})
			pool.Wait()
			if lastErr != nil {
				return lastErr
			}
		}

		return nil
	})

	if err != nil {
		log.Logger.Errorf("Error walking the folder: %v", err)
		return err
	}
	if lastErr != nil {
		log.Logger.Errorf("Error uploading files: %v", lastErr)
		return lastErr
	}
	log.Logger.Infof("%d files upload to s3 success! Elapsed: %v sec", cnt, time.Since(start).Seconds())
	return nil
}

func (c *S3Client) uploadFile(key, fpath, bucket string, dryrun bool) error {
	skey := path.Join(key, filepath.Base(fpath))
	if !dryrun {
		file, err := os.Open(fpath)
		if err != nil {
			log.Logger.Errorf("Error opening file:%v", err)
			return err
		}
		defer file.Close()
		_, err = c.svc.PutObjectWithContext(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(skey),
			Body:   file,
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				log.Logger.Error(err)
				return fmt.Errorf("upload canceled due to timeout, %v", err)
			}
			log.Logger.Error(err)
			return fmt.Errorf("failed to upload object, %v", err)
		}
	}
	log.Logger.Infof("Uploaded:[%s] to [%s]", fpath, skey)
	return nil
}
