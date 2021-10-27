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
	if conf.Password != "" {
		conf.Password = common.AesEncryptECB(conf.Password)
	}
	if conf.SshPassword != "" {
		conf.SshPassword = common.AesEncryptECB(conf.SshPassword)
	}
	for idx, user := range conf.UsersConf.Users {
		if user.Password != "" {
			conf.UsersConf.Users[idx].Password = common.AesEncryptECB(user.Password)
		}
	}
}

func DecodePasswd(conf *model.CKManClickHouseConfig) {
	if conf.Password != "" {
		conf.Password = common.AesDecryptECB(conf.Password)
	}
	if conf.SshPassword != "" {
		conf.SshPassword = common.AesDecryptECB(conf.SshPassword)
	}
	for idx, user := range conf.UsersConf.Users {
		if user.Password != "" {
			conf.UsersConf.Users[idx].Password = common.AesDecryptECB(user.Password)
		}
	}
}
