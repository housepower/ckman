package diagnosis

import (
	"context"
	"errors"
	"testing"
	"time"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

func TestRunProfilePreservesReportWhenCheckReturnsError(t *testing.T) {
	profile := Profile{Scene: "test", Checks: []string{"replica_status", "disk_space_low"}}
	run := func(_ context.Context, id string, _ []diagnose.Target, _ diagnose.CheckOptions) (diagnose.Report, error) {
		switch id {
		case "replica_status":
			return diagnose.ReplicaStatusReport{
				CheckID: id, RuleVersion: "2", Execution: diagnose.ExecutionPartial, Outcome: diagnose.OutcomeMatched,
				Matches: []diagnose.ReplicaMatch{{ShardID: "s1", Database: "db", Table: "t", Observations: []diagnose.ReplicaObservation{{NodeID: "n1", Reasons: []string{diagnose.ReasonKeeperUnreachable}}}}},
			}, context.DeadlineExceeded
		default:
			return diagnose.DiskSpaceReport{CheckID: id, RuleVersion: "1", Execution: diagnose.ExecutionComplete, Outcome: diagnose.OutcomeNoMatch}, nil
		}
	}

	got := runProfile(context.Background(), profile, nil, diagnose.CheckOptions{}, run)
	if got.Execution != diagnose.ExecutionPartial || got.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want partial/matched", got.Execution, got.Outcome)
	}
	if got.Severity != SeverityCritical {
		t.Fatalf("severity = %s, want critical", got.Severity)
	}
	if len(got.Checks) != 2 || len(got.Checks[0].Report) == 0 || got.Checks[0].ExecutionError == "" {
		t.Fatalf("report+error contract lost: %+v", got.Checks)
	}
}

func TestRunProfileAllFailuresIsFailedUnknown(t *testing.T) {
	run := func(context.Context, string, []diagnose.Target, diagnose.CheckOptions) (diagnose.Report, error) {
		return nil, errors.New("connect failed")
	}
	got := runProfile(context.Background(), Profile{Scene: "test", Checks: []string{"replica_status"}}, nil, diagnose.CheckOptions{}, run)
	if got.Execution != diagnose.ExecutionFailed || got.Outcome != diagnose.OutcomeUnknown || got.Severity != SeverityNone {
		t.Fatalf("unexpected aggregate: %+v", got)
	}
}

func TestRunProfileStopsAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	run := func(context.Context, string, []diagnose.Target, diagnose.CheckOptions) (diagnose.Report, error) {
		calls++
		cancel()
		return diagnose.TooManyPartsReport{CheckID: "too_many_parts", RuleVersion: "1", Execution: diagnose.ExecutionPartial, Outcome: diagnose.OutcomeUnknown, StartedAt: time.Now()}, context.Canceled
	}
	got := runProfile(ctx, Profile{Scene: "test", Checks: []string{"too_many_parts", "disk_space_low"}}, nil, diagnose.CheckOptions{}, run)
	if calls != 1 || len(got.Checks) != 1 {
		t.Fatalf("calls=%d checks=%d, want 1", calls, len(got.Checks))
	}
}

func TestSeverityForReasonsUsesHighestLevel(t *testing.T) {
	got := severityForReasons([]string{reasonTooManyParts, diagnose.ReasonReplicaReadonly, diagnose.ReasonDiskReserveBreached})
	if got != SeverityCritical {
		t.Fatalf("severity = %s, want critical", got)
	}
}
