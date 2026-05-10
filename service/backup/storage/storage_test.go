package storage

import (
	"testing"

	"github.com/housepower/ckman/model"
)

// 编译期断言：fakeStorage 满足 Storage
var _ Storage = (*fakeStorage)(nil)

type fakeStorage struct {
	prepared []string
	checked  []string
}

func (f *fakeStorage) Init() error                                             { return nil }
func (f *fakeStorage) BackupSQL(database, table, partition, key string) string { return "" }
func (f *fakeStorage) RestoreSQL(database, table, partition, key string) string {
	return ""
}
func (f *fakeStorage) CleanPartition(database, table, host, partition string) error {
	f.prepared = append(f.prepared, partition)
	return nil
}
func (f *fakeStorage) CheckPartition(host, database, table, partition string,
	pathInfo map[string]model.PathInfo) error {
	f.checked = append(f.checked, partition)
	return nil
}
func (f *fakeStorage) Type() string { return "fake" }

func TestStorage_InterfaceCompiles(t *testing.T) {
	var s Storage = &fakeStorage{}
	if s.Type() != "fake" {
		t.Fatalf("Type")
	}
	if err := s.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	_ = s.BackupSQL("d", "t", "p", "k")
	_ = s.RestoreSQL("d", "t", "p", "k")
	_ = s.CleanPartition("d", "t", "h", "p")
	_ = s.CheckPartition("h", "d", "t", "p", nil)
}
