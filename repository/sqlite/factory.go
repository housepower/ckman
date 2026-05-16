package sqlite

import "github.com/housepower/ckman/repository"

// 注意：init() 暂时注释 —— 在 SQLitePersistent 完成全部 CRUD 方法之前，
// 它不能满足 repository.PersistentMgr 接口，无法注册到 PersistentRegistry。
// 待 WU12 完成 BackupRun CRUD 后，取消下面 init() 的注释，并提交。
//
// func init() {
//     repository.RegistePersistent(NewFactory)
// }

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
	// Body filled in WU12 once full PersistentMgr is implemented.
	// For now, return nil — this is unreachable because init() registration is commented out.
	return nil
}

func (factory *Factory) GetPersistentName() string {
	return SQLitePersistentName
}

func NewFactory() repository.PersistentFactory {
	return &Factory{}
}
