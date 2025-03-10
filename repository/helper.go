package repository

import (
	"fmt"

	"github.com/housepower/ckman/common"
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
	for idx, disk := range conf.Storage.Disks {
		if disk.Type == "s3" {
			conf.Storage.Disks[idx].DiskS3.AccessKeyID = common.AesEncryptECB(disk.DiskS3.AccessKeyID)
			conf.Storage.Disks[idx].DiskS3.SecretAccessKey = common.AesEncryptECB(disk.DiskS3.SecretAccessKey)
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

	var disks []model.Disk
	for _, disk := range conf.Storage.Disks {
		if disk.Type == "s3" {
			disk.DiskS3.AccessKeyID = common.AesDecryptECB(disk.DiskS3.AccessKeyID)
			disk.DiskS3.SecretAccessKey = common.AesDecryptECB(disk.DiskS3.SecretAccessKey)
		}
		disks = append(disks, disk)
	}
	conf.UsersConf.Users = users
	conf.Storage.Disks = disks
}
