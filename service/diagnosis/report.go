package diagnosis

import (
	"context"
	"encoding/json"
	"time"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

type Severity string

const (
	SeverityNone     Severity = "none"
	SeverityWarning  Severity = "warning"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

type CheckResult struct {
	Meta           diagnose.CheckMeta    `json:"meta"`
	Header         diagnose.ReportHeader `json:"header"`
	Report         json.RawMessage       `json:"report,omitempty"`
	ExecutionError string                `json:"execution_error,omitempty"`
}

type RunReport struct {
	Scene      string             `json:"scene"`
	StartedAt  time.Time          `json:"started_at"`
	FinishedAt time.Time          `json:"finished_at"`
	Execution  diagnose.Execution `json:"execution"`
	Outcome    diagnose.Outcome   `json:"outcome"`
	Severity   Severity           `json:"severity"`
	Reasons    []string           `json:"reasons"`
	Checks     []CheckResult      `json:"checks"`
}

type runCheckFunc func(context.Context, string, []diagnose.Target, diagnose.CheckOptions) (diagnose.Report, error)

// RunProfile executes the checks declared by a server-owned scene profile in
// stable order. The report is always considered before the returned error.
func RunProfile(ctx context.Context, profile Profile, targets []diagnose.Target, opts diagnose.CheckOptions) RunReport {
	return runProfile(ctx, profile, targets, opts, diagnose.RunCheck)
}

func runProfile(ctx context.Context, profile Profile, targets []diagnose.Target, opts diagnose.CheckOptions, run runCheckFunc) RunReport {
	result := RunReport{Scene: profile.Scene, StartedAt: time.Now(), Execution: diagnose.ExecutionComplete, Outcome: diagnose.OutcomeNoMatch, Severity: SeverityNone}
	usable := 0
	unknown := false
	matched := false
	reasonSet := make(map[string]struct{})

	for _, checkID := range profile.Checks {
		report, err := run(ctx, checkID, targets, opts)
		entry := CheckResult{Meta: checkMeta(checkID)}
		if report != nil {
			usable++
			entry.Header = report.Header()
			entry.Meta = entry.Header.CheckMeta
			if raw, marshalErr := json.Marshal(report); marshalErr != nil {
				entry.ExecutionError = SanitizeText(marshalErr.Error(), maxExceptionRunes)
				unknown = true
				result.Execution = diagnose.ExecutionPartial
			} else {
				entry.Report = raw
			}
			if entry.Header.Execution != diagnose.ExecutionComplete {
				result.Execution = diagnose.ExecutionPartial
			}
			switch entry.Header.Outcome {
			case diagnose.OutcomeMatched:
				matched = true
			case diagnose.OutcomeUnknown:
				unknown = true
			}
			for _, reason := range reportReasons(report) {
				reasonSet[reason] = struct{}{}
			}
		}
		if err != nil {
			entry.ExecutionError = SanitizeText(err.Error(), maxExceptionRunes)
			unknown = true
			if usable > 0 {
				result.Execution = diagnose.ExecutionPartial
			}
		}
		result.Checks = append(result.Checks, entry)
		if ctx.Err() != nil {
			break
		}
	}

	if usable == 0 {
		result.Execution = diagnose.ExecutionFailed
		result.Outcome = diagnose.OutcomeUnknown
	} else if matched {
		result.Outcome = diagnose.OutcomeMatched
	} else if unknown || result.Execution != diagnose.ExecutionComplete {
		result.Outcome = diagnose.OutcomeUnknown
	}
	result.Reasons = sortedReasons(reasonSet)
	result.Severity = severityForReasons(result.Reasons)
	result.FinishedAt = time.Now()
	return result
}

func checkMeta(id string) diagnose.CheckMeta {
	for _, meta := range diagnose.ListChecks() {
		if meta.ID == id {
			return meta
		}
	}
	return diagnose.CheckMeta{ID: id}
}
