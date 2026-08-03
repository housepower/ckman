package repository

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

var (
	ErrRecordNotFound   = fmt.Errorf("record not found")
	ErrRecordExists     = fmt.Errorf("record is exists already")
	ErrTransActionBegin = fmt.Errorf("transaction already begin")
	ErrTransActionEnd   = fmt.Errorf("transaction already commit or rollback")
)

func EncodePasswd(conf *model.CKManClickHouseConfig) {
	conf.Password = common.AesEncryptECB(conf.Password)
	conf.SshPassword = common.AesEncryptECB(conf.SshPassword)
	for idx, user := range conf.UsersConf.Users {
		conf.UsersConf.Users[idx].Password = common.AesEncryptECB(user.Password)
	}
	if conf.Storage != nil {
		for idx, disk := range conf.Storage.Disks {
			if disk.Type == "s3" && disk.DiskS3 != nil {
				conf.Storage.Disks[idx].DiskS3.AccessKeyID = common.AesEncryptECB(disk.DiskS3.AccessKeyID)
				conf.Storage.Disks[idx].DiskS3.SecretAccessKey = common.AesEncryptECB(disk.DiskS3.SecretAccessKey)
			}
		}
	}
	if conf.TlsConf != nil {
		// 新建对象：入参可能与内存中的集群配置共享同一 TlsConf 指针
		//
		// 私钥先做 base64 再送入 AesEncryptECB：common.AesDecryptECB 用「首个小于
		// 0x20 的字节」当作 PKCS 填充的起点来做截断（见 common/aes.go），PEM 私钥
		// 里的换行符（0x0A）会被当成填充误截断，导致解密后的私钥不完整。
		// base64 之后的字符集不含任何小于 0x20 的字节，可以安全地绕开这个截断逻辑。
		conf.TlsConf = &model.TlsConf{
			Cert:       conf.TlsConf.Cert,
			Key:        common.AesEncryptECB(base64.StdEncoding.EncodeToString([]byte(conf.TlsConf.Key))),
			SelfSigned: conf.TlsConf.SelfSigned,
		}
	}
}

func DecodePasswd(conf *model.CKManClickHouseConfig) {
	conf.Password = common.AesDecryptECB(conf.Password)
	conf.SshPassword = common.AesDecryptECB(conf.SshPassword)

	var users []model.User
	/*
		conf.UsersConf.Users and lp.Data.Clusters[key] pointer the same address when policy is local
		When conf changed, lp.Data will changed,too. which will cause panic when decode password (runtime error: slice bounds out of range [:16] with capacity 3)
		That's not allow to happen, So we need another memory to storage users, to ensure that the original data (lp.Data) not changed when decode password
	*/
	for _, user := range conf.UsersConf.Users {
		user.Password = common.AesDecryptECB(user.Password)
		users = append(users, user)
	}
	conf.UsersConf.Users = users

	if conf.Storage != nil {
		var disks []model.Disk
		for _, disk := range conf.Storage.Disks {
			if disk.Type == "s3" && disk.DiskS3 != nil {
				disk.DiskS3.AccessKeyID = common.AesDecryptECB(disk.DiskS3.AccessKeyID)
				disk.DiskS3.SecretAccessKey = common.AesDecryptECB(disk.DiskS3.SecretAccessKey)
			}
			disks = append(disks, disk)
		}
		conf.Storage.Disks = disks
	}

	if conf.TlsConf != nil {
		// key 先保留 AesDecryptECB 的直接输出：AesDecryptECB 解密失败时会通过
		// defer/recover 把原始密文（大写 hex 字符串）原样吐回来，这本身就是一个
		// 有辨识度的信号（人眼能看出"这是没解开的密文"）。只有确认 base64 还原出的
		// 内容像一份 PEM 私钥（包含 "PRIVATE KEY"）时才用它替换 key；否则保留这个
		// 未经 base64 还原的值,不要用 base64.DecodeString 的"垃圾成功"结果覆盖掉它 ——
		// 那样反而会把一个有辨识度的信号变成一堆无意义乱码,销毁排障线索。
		key := common.AesDecryptECB(conf.TlsConf.Key)
		if key != "" {
			if raw, err := base64.StdEncoding.DecodeString(key); err == nil && strings.Contains(string(raw), "PRIVATE KEY") {
				key = string(raw)
			} else {
				log.Logger.Warnf("cluster[%s] TLS private key decrypt sanity check failed (decrypted/base64-decoded content does not look like a PEM private key); ciphertext may be corrupted or was not produced by EncodePasswd, keeping raw decrypted value for troubleshooting", conf.Cluster)
			}
		}
		conf.TlsConf = &model.TlsConf{
			Cert:       conf.TlsConf.Cert,
			Key:        key,
			SelfSigned: conf.TlsConf.SelfSigned,
		}
	}
}
