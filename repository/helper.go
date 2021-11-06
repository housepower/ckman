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
}
