package sqlite

import (
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

func (sp *SQLitePersistent) CreateDiagnosisRun(r model.DiagnosisRun) error {
	return repository.CreateDiagnosisRun(sp.Client, r)
}
func (sp *SQLitePersistent) GetDiagnosisRun(id string) (model.DiagnosisRun, error) {
	return repository.GetDiagnosisRun(sp.Client, id)
}
func (sp *SQLitePersistent) ListDiagnosisRuns(q model.DiagnosisRunQuery) ([]model.DiagnosisRun, error) {
	return repository.ListDiagnosisRuns(sp.Client, q)
}
func (sp *SQLitePersistent) UpdateDiagnosisRun(r model.DiagnosisRun) error {
	return repository.UpdateDiagnosisRun(sp.Client, r)
}
func (sp *SQLitePersistent) MarkDiagnosisRunRunningIfQueued(id, instance string, at int64) (bool, error) {
	return repository.MarkDiagnosisRunRunningIfQueued(sp.Client, id, instance, at)
}
func (sp *SQLitePersistent) RequestDiagnosisRunCancel(id, user string, at int64) (bool, error) {
	return repository.RequestDiagnosisRunCancel(sp.Client, id, user, at)
}
func (sp *SQLitePersistent) HeartbeatDiagnosisRun(id, instance string, at int64) (bool, error) {
	return repository.HeartbeatDiagnosisRun(sp.Client, id, instance, at)
}
func (sp *SQLitePersistent) MarkStaleDiagnosisRunsInterrupted(before, at int64) (int64, error) {
	return repository.MarkStaleDiagnosisRunsInterrupted(sp.Client, before, at)
}
func (sp *SQLitePersistent) DeleteExpiredDiagnosisRuns(at int64) (int64, error) {
	return repository.DeleteExpiredDiagnosisRuns(sp.Client, at)
}
