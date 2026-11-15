package model

const (
	DiagnosisStatusQueued      = "queued"
	DiagnosisStatusRunning     = "running"
	DiagnosisStatusComplete    = "complete"
	DiagnosisStatusPartial     = "partial"
	DiagnosisStatusFailed      = "failed"
	DiagnosisStatusCanceling   = "canceling"
	DiagnosisStatusCanceled    = "canceled"
	DiagnosisStatusInterrupted = "interrupted"
)

// DiagnosisRun is the durable envelope for one deterministic diagnosis.
// Times are Unix milliseconds to keep all persistence backends consistent.
type DiagnosisRun struct {
	RunID           string `json:"run_id" gorm:"column:run_id;primaryKey;size:64"`
	ClusterName     string `json:"cluster_name" gorm:"column:cluster_name;size:128;index:idx_dr_cluster_created,priority:1"`
	RequestedBy     string `json:"requested_by" gorm:"column:requested_by;size:64;index:idx_dr_user_created,priority:1"`
	Scene           string `json:"scene" gorm:"column:scene;size:64"`
	ContextJSON     string `json:"context_json,omitempty" gorm:"column:context_json;type:TEXT"`
	ChecksJSON      string `json:"checks_json" gorm:"column:checks_json;type:TEXT"`
	Status          string `json:"status" gorm:"column:status;size:16;index:idx_dr_status_updated,priority:1"`
	Execution       string `json:"execution,omitempty" gorm:"column:execution;size:16"`
	Outcome         string `json:"outcome,omitempty" gorm:"column:outcome;size:16"`
	Severity        string `json:"severity,omitempty" gorm:"column:severity;size:16"`
	ReportJSON      string `json:"report_json,omitempty" gorm:"column:report_json;type:TEXT"`
	ErrorMessage    string `json:"error_message,omitempty" gorm:"column:error_message;type:TEXT"`
	OwnerInstance   string `json:"-" gorm:"column:owner_instance;size:128"`
	CancelRequested bool   `json:"cancel_requested" gorm:"column:cancel_requested"`
	CreatedAtMS     int64  `json:"created_at_ms" gorm:"column:created_at_ms;index:idx_dr_cluster_created,priority:2;index:idx_dr_user_created,priority:2"`
	StartedAtMS     int64  `json:"started_at_ms,omitempty" gorm:"column:started_at_ms"`
	FinishedAtMS    int64  `json:"finished_at_ms,omitempty" gorm:"column:finished_at_ms"`
	UpdatedAtMS     int64  `json:"updated_at_ms" gorm:"column:updated_at_ms;index:idx_dr_status_updated,priority:2"`
	ExpiresAtMS     int64  `json:"expires_at_ms" gorm:"column:expires_at_ms;index:idx_dr_expires"`
}

func (DiagnosisRun) TableName() string { return "tbl_diagnosis_run" }

type DiagnosisRunQuery struct {
	ClusterName string
	RequestedBy string
	Status      string
	Scene       string
	Limit       int
	Offset      int
}
