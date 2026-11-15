package diagnosis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/model"
)

const (
	maxStoredReportBytes = 2 << 20
	defaultRetention     = 30 * 24 * time.Hour
)

type RunRepository interface {
	CreateDiagnosisRun(model.DiagnosisRun) error
	GetDiagnosisRun(string) (model.DiagnosisRun, error)
	ListDiagnosisRuns(model.DiagnosisRunQuery) ([]model.DiagnosisRun, error)
	UpdateDiagnosisRun(model.DiagnosisRun) error
	MarkDiagnosisRunRunningIfQueued(string, string, int64) (bool, error)
	RequestDiagnosisRunCancel(string, string, int64) (bool, error)
	HeartbeatDiagnosisRun(string, string, int64) (bool, error)
	MarkStaleDiagnosisRunsInterrupted(int64, int64) (int64, error)
}

type ClusterRepository interface {
	GetClusterbyName(string) (model.CKManClickHouseConfig, error)
}

type Repository interface {
	RunRepository
	ClusterRepository
}

type RunContext struct {
	Database string `json:"database,omitempty"`
	Table    string `json:"table,omitempty"`
	QueryID  string `json:"query_id,omitempty"`
	DDLEntry string `json:"ddl_entry,omitempty"`
}

type StartRequest struct {
	Scene   string     `json:"scene"`
	Context RunContext `json:"context,omitempty"`
}

type Orchestrator struct {
	repo     Repository
	instance string
	sem      chan struct{}
	cancels  sync.Map
}

func NewOrchestrator(repo Repository, instance string, workers int) *Orchestrator {
	if workers < 1 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	orchestrator := &Orchestrator{repo: repo, instance: instance, sem: make(chan struct{}, workers)}
	now := time.Now().UnixMilli()
	_, _ = repo.MarkStaleDiagnosisRunsInterrupted(now-30_000, now)
	return orchestrator
}

func (o *Orchestrator) StartRun(cluster, user string, req StartRequest) (model.DiagnosisRun, error) {
	profile, err := ResolveProfile(req.Scene)
	if err != nil {
		return model.DiagnosisRun{}, err
	}
	if cluster == "" || user == "" {
		return model.DiagnosisRun{}, errors.New("diagnosis: cluster and user are required")
	}
	if len(req.Context.Database) > 256 || len(req.Context.Table) > 256 || len(req.Context.QueryID) > 256 || len(req.Context.DDLEntry) > 512 {
		return model.DiagnosisRun{}, errors.New("diagnosis: context field too long")
	}
	if profile.RequiresTable && (req.Context.Database == "" || req.Context.Table == "") {
		return model.DiagnosisRun{}, errors.New("diagnosis: scene requires database and table context")
	}
	if profile.RequiresQueryID && req.Context.QueryID == "" {
		return model.DiagnosisRun{}, errors.New("diagnosis: scene requires query_id context")
	}
	if profile.RequiresDDLEntry && req.Context.DDLEntry == "" {
		return model.DiagnosisRun{}, errors.New("diagnosis: scene requires ddl_entry context")
	}
	if _, err := o.repo.GetClusterbyName(cluster); err != nil {
		return model.DiagnosisRun{}, err
	}
	contextRaw, _ := json.Marshal(req.Context)
	checksRaw, _ := json.Marshal(profile.Checks)
	now := time.Now()
	run := model.DiagnosisRun{RunID: uuid.New(), ClusterName: cluster, RequestedBy: user, Scene: req.Scene,
		ContextJSON: string(contextRaw), ChecksJSON: string(checksRaw), Status: model.DiagnosisStatusQueued,
		CreatedAtMS: now.UnixMilli(), UpdatedAtMS: now.UnixMilli(), ExpiresAtMS: now.Add(defaultRetention).UnixMilli()}
	if err := o.repo.CreateDiagnosisRun(run); err != nil {
		return model.DiagnosisRun{}, err
	}
	go o.execute(run.RunID)
	return run, nil
}

func (o *Orchestrator) execute(runID string) {
	o.sem <- struct{}{}
	defer func() { <-o.sem }()
	now := time.Now().UnixMilli()
	claimed, err := o.repo.MarkDiagnosisRunRunningIfQueued(runID, o.instance, now)
	if err != nil || !claimed {
		return
	}
	run, err := o.repo.GetDiagnosisRun(runID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	o.cancels.Store(runID, cancel)
	defer func() { cancel(); o.cancels.Delete(runID) }()
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)
	go o.heartbeat(ctx, cancel, runID, heartbeatDone)
	conf, err := o.repo.GetClusterbyName(run.ClusterName)
	if err != nil {
		o.fail(run, err)
		return
	}
	targets, err := BuildTargets(conf)
	if err != nil {
		o.fail(run, err)
		return
	}
	profile, err := ResolveProfile(run.Scene)
	if err != nil {
		o.fail(run, err)
		return
	}
	var runContext RunContext
	if err := json.Unmarshal([]byte(run.ContextJSON), &runContext); err != nil {
		o.fail(run, fmt.Errorf("diagnosis context: %w", err))
		return
	}
	report := RunProfile(ctx, profile, targets, diagnose.CheckOptions{
		Timeout: 2 * time.Minute, NodeTimeout: 30 * time.Second, Concurrency: 4, MaxRowsPerNode: 1000,
		Scope: diagnose.Scope{Database: runContext.Database, Table: runContext.Table}, QueryID: runContext.QueryID, DDLEntry: runContext.DDLEntry,
	})
	raw, err := json.Marshal(report)
	if err != nil {
		o.fail(run, err)
		return
	}
	if len(raw) > maxStoredReportBytes {
		o.fail(run, fmt.Errorf("diagnosis report exceeds %d bytes", maxStoredReportBytes))
		return
	}
	run.ReportJSON = string(raw)
	run.Execution = string(report.Execution)
	run.Outcome = string(report.Outcome)
	run.Severity = string(report.Severity)
	run.FinishedAtMS = time.Now().UnixMilli()
	run.UpdatedAtMS = run.FinishedAtMS
	if ctx.Err() != nil || run.CancelRequested {
		run.Status = model.DiagnosisStatusCanceled
		run.CancelRequested = true
	} else if report.Execution == diagnose.ExecutionFailed {
		run.Status = model.DiagnosisStatusFailed
	} else if report.Execution == diagnose.ExecutionPartial {
		run.Status = model.DiagnosisStatusPartial
	} else {
		run.Status = model.DiagnosisStatusComplete
	}
	_ = o.repo.UpdateDiagnosisRun(run)
}

func (o *Orchestrator) heartbeat(ctx context.Context, cancel context.CancelFunc, runID string, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			ok, err := o.repo.HeartbeatDiagnosisRun(runID, o.instance, now.UnixMilli())
			if err != nil || !ok {
				cancel()
				return
			}
			run, err := o.repo.GetDiagnosisRun(runID)
			if err != nil || run.CancelRequested {
				cancel()
				return
			}
		}
	}
}

func (o *Orchestrator) fail(run model.DiagnosisRun, err error) {
	run.Status = model.DiagnosisStatusFailed
	run.ErrorMessage = SanitizeText(err.Error(), maxExceptionRunes)
	run.FinishedAtMS = time.Now().UnixMilli()
	run.UpdatedAtMS = run.FinishedAtMS
	_ = o.repo.UpdateDiagnosisRun(run)
}

func (o *Orchestrator) Cancel(run model.DiagnosisRun) (bool, error) {
	ok, err := o.repo.RequestDiagnosisRunCancel(run.RunID, run.RequestedBy, time.Now().UnixMilli())
	if err == nil && ok {
		if run.Status == model.DiagnosisStatusQueued {
			run.Status = model.DiagnosisStatusCanceled
			run.CancelRequested = true
			run.FinishedAtMS = time.Now().UnixMilli()
			run.UpdatedAtMS = run.FinishedAtMS
			if updateErr := o.repo.UpdateDiagnosisRun(run); updateErr != nil {
				return false, updateErr
			}
		}
		if value, exists := o.cancels.Load(run.RunID); exists {
			value.(context.CancelFunc)()
		}
	}
	return ok, err
}
