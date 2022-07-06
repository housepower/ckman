package dm8

import "github.com/housepower/ckman/repository"

func init() {
	repository.RegistePersistent(NewFactory)
}

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
	return NewDM8Persistent()
}

func (factory *Factory) GetPersistentName() string {
	return DM8PersistentName
}

func NewFactory() repository.PersistentFactory {
	return &Factory{}
}
