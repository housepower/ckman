package repository

import (
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func CreateDiagnosisRun(db *gorm.DB, run model.DiagnosisRun) error {
	return errors.Wrap(db.Create(&run).Error, "")
}

func GetDiagnosisRun(db *gorm.DB, runID string) (model.DiagnosisRun, error) {
	var run model.DiagnosisRun
	if err := db.Where("run_id = ?", runID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return run, ErrRecordNotFound
		}
		return run, errors.Wrap(err, "")
	}
	return run, nil
}

func ListDiagnosisRuns(db *gorm.DB, q model.DiagnosisRunQuery) ([]model.DiagnosisRun, error) {
	tx := db.Model(&model.DiagnosisRun{})
	if q.ClusterName != "" {
		tx = tx.Where("cluster_name = ?", q.ClusterName)
	}
	if q.RequestedBy != "" {
		tx = tx.Where("requested_by = ?", q.RequestedBy)
	}
	if q.Status != "" {
		tx = tx.Where("status = ?", q.Status)
	}
	if q.Scene != "" {
		tx = tx.Where("scene = ?", q.Scene)
	}
	limit := q.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var runs []model.DiagnosisRun
	err := tx.Order("created_at_ms DESC").Limit(limit).Offset(q.Offset).Find(&runs).Error
	return runs, errors.Wrap(err, "")
}

func UpdateDiagnosisRun(db *gorm.DB, run model.DiagnosisRun) error {
	tx := db.Model(&model.DiagnosisRun{}).Where("run_id = ?", run.RunID).Select("*").Updates(&run)
	if tx.Error != nil {
		return errors.Wrap(tx.Error, "")
	}
	if tx.RowsAffected == 0 {
		return ErrRecordNotFound
	}
	return nil
}

func MarkDiagnosisRunRunningIfQueued(db *gorm.DB, runID, instance string, startedAtMS int64) (bool, error) {
	tx := db.Model(&model.DiagnosisRun{}).Where("run_id = ? AND status = ?", runID, model.DiagnosisStatusQueued).Updates(map[string]interface{}{
		"status": model.DiagnosisStatusRunning, "owner_instance": instance, "started_at_ms": startedAtMS, "updated_at_ms": startedAtMS,
	})
	return tx.RowsAffected == 1, errors.Wrap(tx.Error, "")
}

func RequestDiagnosisRunCancel(db *gorm.DB, runID, requestedBy string, updatedAtMS int64) (bool, error) {
	tx := db.Model(&model.DiagnosisRun{}).Where("run_id = ? AND requested_by = ? AND status IN ?", runID, requestedBy,
		[]string{model.DiagnosisStatusQueued, model.DiagnosisStatusRunning}).Updates(map[string]interface{}{
		"cancel_requested": true, "status": model.DiagnosisStatusCanceling, "updated_at_ms": updatedAtMS,
	})
	return tx.RowsAffected == 1, errors.Wrap(tx.Error, "")
}

func HeartbeatDiagnosisRun(db *gorm.DB, runID, instance string, updatedAtMS int64) (bool, error) {
	tx := db.Model(&model.DiagnosisRun{}).Where("run_id = ? AND owner_instance = ? AND status IN ?", runID, instance,
		[]string{model.DiagnosisStatusRunning, model.DiagnosisStatusCanceling}).Update("updated_at_ms", updatedAtMS)
	return tx.RowsAffected == 1, errors.Wrap(tx.Error, "")
}

func MarkStaleDiagnosisRunsInterrupted(db *gorm.DB, staleBeforeMS, finishedAtMS int64) (int64, error) {
	tx := db.Model(&model.DiagnosisRun{}).Where("status IN ? AND updated_at_ms < ?",
		[]string{model.DiagnosisStatusRunning, model.DiagnosisStatusCanceling}, staleBeforeMS).Updates(map[string]interface{}{
		"status": model.DiagnosisStatusInterrupted, "finished_at_ms": finishedAtMS, "updated_at_ms": finishedAtMS,
	})
	return tx.RowsAffected, errors.Wrap(tx.Error, "")
}

func DeleteExpiredDiagnosisRuns(db *gorm.DB, nowMS int64) (int64, error) {
	tx := db.Where("expires_at_ms > 0 AND expires_at_ms < ?", nowMS).Delete(&model.DiagnosisRun{})
	return tx.RowsAffected, errors.Wrap(tx.Error, "")
}
