package diagnosis

import (
	"sort"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

const (
	reasonNoReplicaHasPart = "no_replica_has_part"
	reasonTooManyParts     = "too_many_parts"
)

func reportReasons(report diagnose.Report) []string {
	set := make(map[string]struct{})
	add := func(reasons ...string) {
		for _, reason := range reasons {
			if reason != "" {
				set[reason] = struct{}{}
			}
		}
	}

	switch report := report.(type) {
	case diagnose.ReplicationQueueReport:
		if len(report.Matches) > 0 {
			add(reasonNoReplicaHasPart)
		}
	case diagnose.TooManyPartsReport:
		if len(report.Matches) > 0 {
			add(reasonTooManyParts)
		}
	case diagnose.ReplicaStatusReport:
		for _, match := range report.Matches {
			for _, observation := range match.Observations {
				add(observation.Reasons...)
			}
		}
	case diagnose.MutationsStuckReport:
		for _, match := range report.Matches {
			for _, observation := range match.Observations {
				add(observation.Reasons...)
			}
		}
	case diagnose.QueueBacklogReport:
		for _, match := range report.Matches {
			for _, observation := range match.Observations {
				add(observation.Reasons...)
			}
		}
	case diagnose.DiskSpaceReport:
		for _, match := range report.Matches {
			for _, observation := range match.Observations {
				add(observation.Reasons...)
			}
		}
	case diagnose.DetachedPartsReport:
		for _, match := range report.Matches {
			add(match.Reasons...)
			for _, observation := range match.Observations {
				add(observation.Reasons...)
			}
		}
	case diagnose.ResourcePressureReport:
		for _, match := range report.Matches {
			add(match.Reasons...)
		}
	case diagnose.SlowQueryReport:
		for _, match := range report.Matches {
			add(match.Reasons...)
		}
	case diagnose.DistributedDDLReport:
		for _, match := range report.Matches {
			add(match.Reasons...)
		}
	}
	return sortedReasons(set)
}

func sortedReasons(set map[string]struct{}) []string {
	reasons := make([]string, 0, len(set))
	for reason := range set {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	return reasons
}

func severityForReasons(reasons []string) Severity {
	severity := SeverityNone
	for _, reason := range reasons {
		candidate := SeverityWarning
		switch reason {
		case diagnose.ReasonDiskReserveBreached, diagnose.ReasonLostParts, diagnose.ReasonKeeperUnreachable:
			candidate = SeverityCritical
		case diagnose.ReasonReplicaReadonly, diagnose.ReasonSessionExpired, diagnose.ReasonMutationFailing, reasonNoReplicaHasPart, diagnose.ReasonDistributedDDLStuck:
			candidate = SeverityHigh
		}
		if severityRank(candidate) > severityRank(severity) {
			severity = candidate
		}
	}
	return severity
}

func severityRank(severity Severity) int {
	switch severity {
	case SeverityCritical:
		return 3
	case SeverityHigh:
		return 2
	case SeverityWarning:
		return 1
	default:
		return 0
	}
}
