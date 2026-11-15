package postgres

import (
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

func (mp *PostgresPersistent) CreateDiagnosisRun(r model.DiagnosisRun) error {
	return repository.CreateDiagnosisRun(mp.Client, r)
}
func (mp *PostgresPersistent) GetDiagnosisRun(id string) (model.DiagnosisRun, error) {
	return repository.GetDiagnosisRun(mp.Client, id)
}
func (mp *PostgresPersistent) ListDiagnosisRuns(q model.DiagnosisRunQuery) ([]model.DiagnosisRun, error) {
	return repository.ListDiagnosisRuns(mp.Client, q)
}
func (mp *PostgresPersistent) UpdateDiagnosisRun(r model.DiagnosisRun) error {
	return repository.UpdateDiagnosisRun(mp.Client, r)
}
func (mp *PostgresPersistent) MarkDiagnosisRunRunningIfQueued(id, instance string, at int64) (bool, error) {
	return repository.MarkDiagnosisRunRunningIfQueued(mp.Client, id, instance, at)
}
func (mp *PostgresPersistent) RequestDiagnosisRunCancel(id, user string, at int64) (bool, error) {
	return repository.RequestDiagnosisRunCancel(mp.Client, id, user, at)
}
func (mp *PostgresPersistent) HeartbeatDiagnosisRun(id, instance string, at int64) (bool, error) {
	return repository.HeartbeatDiagnosisRun(mp.Client, id, instance, at)
}
func (mp *PostgresPersistent) MarkStaleDiagnosisRunsInterrupted(before, at int64) (int64, error) {
	return repository.MarkStaleDiagnosisRunsInterrupted(mp.Client, before, at)
}
func (mp *PostgresPersistent) DeleteExpiredDiagnosisRuns(at int64) (int64, error) {
	return repository.DeleteExpiredDiagnosisRuns(mp.Client, at)
}
