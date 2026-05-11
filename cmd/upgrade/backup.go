package upgrade

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
)

// PersistentForUpgrade is the minimal interface needed for the migration.
// Using an interface allows the core logic to be tested with a fake implementation.
type PersistentForUpgrade interface {
	GetAllClusters() (map[string]model.CKManClickHouseConfig, error)
	GetAllBackups(cluster string) ([]model.Backup, error)
	GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error)
	CreateBackupPolicy(p model.BackupPolicy) error
	CreateBackupRun(r model.BackupRun) error
	DeleteBackup(id string) error
}

// BackupUpgradeOpts holds the CLI options for the upgrade backup command.
type BackupUpgradeOpts struct {
	ConfigFile string
	DryRun     bool
	Verbose    bool
	Force      bool   // allow writing even when new tables are non-empty
	Cleanup    bool   // delete migrated old Backup rows after success
}

// Stats holds migration counters returned by runMigration.
type Stats struct {
	ClustersScanned    int
	OldBackupRows      int
	PoliciesCreated    int
	RunsCreated        int
	Skipped            int
	LegacyPlaceholders int // restore rows whose backup policy could not be found
	CleanupRowsDeleted int
}

// policyKey is the grouping key for old Backup rows → one BackupPolicy.
type policyKey struct {
	ClusterName  string
	Database     string
	Table        string
	ScheduleType string
	Crontab      string
	Instance     string
}

// mapStatus converts an old Backup status to a new BackupRun status.
func mapStatus(old string) string {
	switch old {
	case model.BACKUP_STATUS_SUCCESS:
		return model.BACKUP_STATUS_SUCCESS
	case model.BACKUP_STATUS_FAILED:
		return model.BACKUP_STATUS_FAILED
	default:
		// waiting, init, prepare, backup, restore, check, close, "" → interrupted
		return model.BACKUP_STATUS_INTERRUPTED
	}
}

// isTerminal returns true for statuses that represent a finished run.
func isTerminal(status string) bool {
	return status == model.BACKUP_STATUS_SUCCESS || status == model.BACKUP_STATUS_FAILED
}

// inferBackupStyle returns BACKUP_STYLE_FULL if any partition in the group is "all",
// otherwise BACKUP_STYLE_INCR.
func inferBackupStyle(rows []model.Backup) string {
	for _, b := range rows {
		for _, p := range b.Partitions {
			if p.Partition == "all" {
				return model.BACKUP_STYLE_FULL
			}
		}
	}
	return model.BACKUP_STYLE_INCR
}

// inferBackupType returns BACKUP_TYPE_DAILY if any row has DaysBefore > 0,
// otherwise BACKUP_TYPE_PARTITION.
func inferBackupType(rows []model.Backup) string {
	for _, b := range rows {
		if b.DaysBefore > 0 {
			return model.BACKUP_TYPE_DAILY
		}
	}
	return model.BACKUP_TYPE_PARTITION
}

// collectPartitions returns a deduplicated list of partition names from a group of rows.
func collectPartitions(rows []model.Backup) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, b := range rows {
		for _, p := range b.Partitions {
			if _, ok := seen[p.Partition]; !ok {
				seen[p.Partition] = struct{}{}
				result = append(result, p.Partition)
			}
		}
	}
	return result
}

// buildPolicy constructs a BackupPolicy from a group of old Backup rows.
func buildPolicy(key policyKey, rows []model.Backup, policyID string) model.BackupPolicy {
	scheduleType := key.ScheduleType
	if scheduleType == "" {
		scheduleType = model.BACKUP_IMMEDIATE
	}

	// Use the earliest CreateTime for policy CreateTime, latest UpdateTime for UpdateTime.
	earliest := rows[0].CreateTime
	latest := rows[0].UpdateTime
	for _, b := range rows[1:] {
		if b.CreateTime.Before(earliest) {
			earliest = b.CreateTime
		}
		if b.UpdateTime.After(latest) {
			latest = b.UpdateTime
		}
	}

	// Pick representative row (first) for single-value fields.
	rep := rows[0]

	return model.BackupPolicy{
		PolicyID:     policyID,
		ClusterName:  key.ClusterName,
		Database:     key.Database,
		Table:        key.Table,
		ScheduleType: scheduleType,
		Crontab:      key.Crontab,
		Instance:     key.Instance,
		BackupStyle:  inferBackupStyle(rows),
		BackupType:   inferBackupType(rows),
		DaysBefore:   rep.DaysBefore,
		Partitions:   collectPartitions(rows),
		TargetType:   rep.TargetType,
		S3:           rep.S3,
		Local:        rep.Local,
		Compression:  rep.Compression,
		Checksum:     rep.Checksum,
		Clean:        rep.Clean,
		Enabled:      true,
		Deleted:      false,
		CreateTime:   earliest,
		UpdateTime:   latest,
	}
}

// buildRun constructs a BackupRun from a single old Backup row.
func buildRun(b model.Backup, policyID string) model.BackupRun {
	// Convert Partitions
	var runPartitions []model.BackupRunPartition
	for _, p := range b.Partitions {
		runPartitions = append(runPartitions, model.BackupRunPartition{
			Partition: p.Partition,
			Status:    p.Status,
			Size:      p.Size,
			Rows:      p.Rows,
			FileNum:   p.FileNum,
			Elapsed:   p.Elapsed,
			Msg:       p.Msg,
		})
	}

	// Sum elapsed
	totalElapsed := 0
	for _, p := range b.Partitions {
		totalElapsed += p.Elapsed
	}

	// Collect error messages from failed partitions
	var errMsgs []string
	for _, p := range b.Partitions {
		if p.Status == model.BACKUP_PARTITION_STATUS_FAILED && p.Msg != "" {
			errMsgs = append(errMsgs, p.Msg)
		}
	}

	newStatus := mapStatus(b.Status)

	var finishedAt time.Time
	if isTerminal(newStatus) {
		finishedAt = b.UpdateTime
	}

	return model.BackupRun{
		RunID:        b.BackupId,
		PolicyID:     policyID,
		ClusterName:  b.ClusterName,
		Database:     b.Database,
		Table:        b.Table,
		Operation:    b.Operation,
		TriggerType:  model.TRIGGER_MIGRATED,
		Instance:     b.Instance,
		Status:       newStatus,
		StatusReason: "migrated from legacy",
		Partitions:   runPartitions,
		StartedAt:    b.CreateTime,
		FinishedAt:   finishedAt,
		Elapsed:      totalElapsed,
		ErrorMsg:     strings.Join(errMsgs, "\n"),
		CreateTime:   b.CreateTime,
	}
}

// runMigration performs the actual migration logic.
// It returns Stats and a non-nil error only on fatal (abort) conditions.
func runMigration(ps PersistentForUpgrade, opts BackupUpgradeOpts) (Stats, error) {
	var stats Stats

	// 1. Check if new tables are already populated (unless --force).
	if !opts.Force {
		allClustersMap, err := ps.GetAllClusters()
		if err != nil {
			return stats, fmt.Errorf("GetAllClusters failed: %w", err)
		}
		for clusterName := range allClustersMap {
			existing, err := ps.GetBackupPoliciesByCluster(clusterName)
			if err != nil {
				return stats, fmt.Errorf("GetBackupPoliciesByCluster(%s) failed: %w", clusterName, err)
			}
			if len(existing) > 0 {
				return stats, fmt.Errorf("new BackupPolicy table already has data for cluster %q; use --force to proceed anyway", clusterName)
			}
		}
	}

	// 2. Gather all old Backup rows per cluster.
	allClustersMap, err := ps.GetAllClusters()
	if err != nil {
		return stats, fmt.Errorf("GetAllClusters failed: %w", err)
	}

	stats.ClustersScanned = len(allClustersMap)

	// Collect all backup rows and also build a lookup for restore handling.
	// backupPolicyByClusterTable: (cluster,db,table) → policyID (for operation=backup groups)
	type tableKey struct {
		ClusterName string
		Database    string
		Table       string
	}
	backupPolicyByTable := make(map[tableKey]string)

	// Collect rows for all clusters.
	type clusterRows struct {
		name string
		rows []model.Backup
	}
	var allClusterRows []clusterRows
	for clusterName := range allClustersMap {
		rows, err := ps.GetAllBackups(clusterName)
		if err != nil {
			fmt.Printf("[ERROR] GetAllBackups(%s) failed: %v — skipping cluster\n", clusterName, err)
			continue
		}
		stats.OldBackupRows += len(rows)
		allClusterRows = append(allClusterRows, clusterRows{name: clusterName, rows: rows})
	}

	// 3. For each cluster, separate backup vs restore rows, group backup rows, create policies.
	// We need two passes: first pass builds backup policies (so restore rows can find them).

	// Phase A: group operation=backup rows, create policies.
	type pendingPolicy struct {
		policy model.BackupPolicy
		rows   []model.Backup // backup rows in this group
	}
	var allPolicies []pendingPolicy

	// Also collect all restore rows globally.
	var allRestoreRows []model.Backup

	for _, cr := range allClusterRows {
		// Separate backup from restore, and skip rows with empty cluster or nil partitions.
		groups := make(map[policyKey][]model.Backup)
		for _, b := range cr.rows {
			if b.ClusterName == "" {
				fmt.Printf("[WARN] skip row backup_id=%s: empty ClusterName\n", b.BackupId)
				stats.Skipped++
				continue
			}
			if len(b.Partitions) == 0 {
				fmt.Printf("[WARN] skip row backup_id=%s cluster=%s: empty Partitions\n", b.BackupId, b.ClusterName)
				stats.Skipped++
				continue
			}

			if b.Operation == model.OP_RESTORE {
				allRestoreRows = append(allRestoreRows, b)
				continue
			}

			key := policyKey{
				ClusterName:  b.ClusterName,
				Database:     b.Database,
				Table:        b.Table,
				ScheduleType: b.ScheduleType,
				Crontab:      b.Crontab,
				Instance:     b.Instance,
			}
			groups[key] = append(groups[key], b)
		}

		// Create one policy per group.
		for key, rows := range groups {
			policyID := uuid.New()
			policy := buildPolicy(key, rows, policyID)
			allPolicies = append(allPolicies, pendingPolicy{policy: policy, rows: rows})

			// Record the first policyID for this (cluster,db,table) for restore lookup.
			tk := tableKey{ClusterName: key.ClusterName, Database: key.Database, Table: key.Table}
			if _, exists := backupPolicyByTable[tk]; !exists {
				backupPolicyByTable[tk] = policyID
			}
		}
	}

	// Phase B: handle restore rows — find matching backup policy or create a placeholder.
	// placeholderPolicies: tableKey → placeholder BackupPolicy (created at most once per table).
	placeholderPolicies := make(map[tableKey]model.BackupPolicy)

	for _, b := range allRestoreRows {
		tk := tableKey{ClusterName: b.ClusterName, Database: b.Database, Table: b.Table}
		if _, found := backupPolicyByTable[tk]; !found {
			// Create a placeholder policy if not already done for this table.
			if _, already := placeholderPolicies[tk]; !already {
				placeholderID := fmt.Sprintf("policy-legacy-%s-%s-%s", b.ClusterName, b.Database, b.Table)
				placeholder := model.BackupPolicy{
					PolicyID:    placeholderID,
					ClusterName: b.ClusterName,
					Database:    b.Database,
					Table:       b.Table,
					Enabled:     false,
					Deleted:     false,
					CreateTime:  b.CreateTime,
					UpdateTime:  b.UpdateTime,
				}
				placeholderPolicies[tk] = placeholder
				backupPolicyByTable[tk] = placeholderID
				stats.LegacyPlaceholders++
				fmt.Printf("[WARN] restore row backup_id=%s has no matching backup policy — created placeholder policy_id=%s\n",
					b.BackupId, placeholderID)
			}
		}
	}

	// 4. Dry-run or write.
	if opts.DryRun {
		// Just log what would happen.
		for _, pp := range allPolicies {
			fmt.Printf("[dry-run] would create BackupPolicy policy_id=%s cluster=%s db=%s table=%s style=%s type=%s\n",
				pp.policy.PolicyID, pp.policy.ClusterName, pp.policy.Database, pp.policy.Table,
				pp.policy.BackupStyle, pp.policy.BackupType)
			for _, b := range pp.rows {
				newStatus := mapStatus(b.Status)
				fmt.Printf("[dry-run]   would create BackupRun run_id=%s policy_id=%s status=%s\n",
					b.BackupId, pp.policy.PolicyID, newStatus)
			}
		}
		for _, ph := range placeholderPolicies {
			fmt.Printf("[dry-run] would create placeholder BackupPolicy policy_id=%s cluster=%s db=%s table=%s\n",
				ph.PolicyID, ph.ClusterName, ph.Database, ph.Table)
		}
		for _, b := range allRestoreRows {
			tk := tableKey{ClusterName: b.ClusterName, Database: b.Database, Table: b.Table}
			policyID := backupPolicyByTable[tk]
			newStatus := mapStatus(b.Status)
			fmt.Printf("[dry-run]   would create restore BackupRun run_id=%s policy_id=%s status=%s\n",
				b.BackupId, policyID, newStatus)
		}
		fmt.Println("\nDRY RUN: no data written")
		stats.PoliciesCreated = len(allPolicies) + len(placeholderPolicies)
		stats.RunsCreated = len(allRestoreRows)
		for _, pp := range allPolicies {
			stats.RunsCreated += len(pp.rows)
		}
		return stats, nil
	}

	// Write backup policies.
	for _, pp := range allPolicies {
		if err := ps.CreateBackupPolicy(pp.policy); err != nil {
			fmt.Printf("[ERROR] CreateBackupPolicy policy_id=%s: %v\n", pp.policy.PolicyID, err)
			// Non-fatal: count skips for all runs in this group.
			stats.Skipped += len(pp.rows)
			continue
		}
		stats.PoliciesCreated++

		// Write runs for this policy.
		for _, b := range pp.rows {
			run := buildRun(b, pp.policy.PolicyID)
			if opts.Verbose {
				fmt.Printf("[migrate] cluster=%s db=%s table=%s backup_id=%s → policy=%s run=%s status=%s\n",
					b.ClusterName, b.Database, b.Table, b.BackupId, pp.policy.PolicyID, run.RunID, run.Status)
			}
			if err := ps.CreateBackupRun(run); err != nil {
				fmt.Printf("[ERROR] CreateBackupRun run_id=%s: %v\n", run.RunID, err)
				stats.Skipped++
				continue
			}
			stats.RunsCreated++
		}
	}

	// Write placeholder policies.
	for _, ph := range placeholderPolicies {
		if err := ps.CreateBackupPolicy(ph); err != nil {
			fmt.Printf("[ERROR] CreateBackupPolicy (placeholder) policy_id=%s: %v\n", ph.PolicyID, err)
		} else {
			stats.PoliciesCreated++
		}
	}

	// Write restore runs.
	for _, b := range allRestoreRows {
		tk := tableKey{ClusterName: b.ClusterName, Database: b.Database, Table: b.Table}
		policyID := backupPolicyByTable[tk]
		run := buildRun(b, policyID)
		if opts.Verbose {
			fmt.Printf("[migrate] cluster=%s db=%s table=%s backup_id=%s → policy=%s run=%s status=%s (restore)\n",
				b.ClusterName, b.Database, b.Table, b.BackupId, policyID, run.RunID, run.Status)
		}
		if err := ps.CreateBackupRun(run); err != nil {
			fmt.Printf("[ERROR] CreateBackupRun (restore) run_id=%s: %v\n", run.RunID, err)
			stats.Skipped++
			continue
		}
		stats.RunsCreated++
	}

	// 5. Cleanup: delete migrated old rows if requested.
	if opts.Cleanup {
		for _, cr := range allClusterRows {
			for _, b := range cr.rows {
				if b.ClusterName == "" || len(b.Partitions) == 0 {
					continue // was already skipped — don't try to delete
				}
				if err := ps.DeleteBackup(b.BackupId); err != nil {
					fmt.Printf("[WARN] DeleteBackup id=%s: %v\n", b.BackupId, err)
				} else {
					stats.CleanupRowsDeleted++
				}
			}
		}
	}

	return stats, nil
}

// BackupUpgradeHandle is the CLI entry point for `ckmanctl upgrade backup`.
func BackupUpgradeHandle(opts BackupUpgradeOpts) {
	log.InitLoggerDefault("info", []string{"/var/log/ckmanctl.log"})

	// Load ckman.hjson into GlobalConfig.
	if err := config.ParseConfigFile(opts.ConfigFile, ""); err != nil {
		fmt.Printf("parse config file %s failed: %v\n", opts.ConfigFile, err)
		os.Exit(3)
	}

	// Apply gsypt (secret decryption) if configured.
	if err := common.Gsypt.Unmarshal(&config.GlobalConfig); err != nil {
		fmt.Printf("gsypt unmarshal failed: %v\n", err)
		os.Exit(3)
	}

	// Init persistent layer using the standard policy from GlobalConfig.
	if err := repository.InitPersistent(); err != nil {
		fmt.Printf("init persistent failed: %v\n", err)
		os.Exit(3)
	}

	// Wrap repository.Ps as PersistentForUpgrade.
	ps := repository.Ps

	stats, err := runMigration(ps, opts)
	printSummary(stats, opts)

	if err != nil {
		fmt.Printf("\nABORTED: %v\n", err)
		os.Exit(2)
	}

	exitCode := 0
	if stats.Skipped > 0 {
		exitCode = 1
	}
	os.Exit(exitCode)
}

// printSummary prints the migration statistics.
func printSummary(stats Stats, opts BackupUpgradeOpts) {
	fmt.Println("\n=== Migration Summary ===")
	fmt.Printf("  Clusters scanned    : %d\n", stats.ClustersScanned)
	fmt.Printf("  Old Backup rows     : %d\n", stats.OldBackupRows)
	fmt.Printf("  Policies created    : %d\n", stats.PoliciesCreated)
	fmt.Printf("  Runs created        : %d\n", stats.RunsCreated)
	fmt.Printf("  Skipped (warnings)  : %d\n", stats.Skipped)
	fmt.Printf("  Legacy placeholders : %d\n", stats.LegacyPlaceholders)
	if opts.Cleanup {
		fmt.Printf("  Cleanup rows deleted: %d\n", stats.CleanupRowsDeleted)
	}
	if opts.DryRun {
		fmt.Println("\nDRY RUN: no data written")
	}
}
