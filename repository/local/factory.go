package local

import "github.com/housepower/ckman/repository"

func init() {
	repository.RegistePersistent(NewFactory)
}

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
	return NewLocalPersistent()
}

func (factory *Factory) GetPersistentName() string {
	return LocalPersistentName
}

func NewFactory() repository.PersistentFactory {
	return &Factory{}
}