package sqlite

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/housepower/ckman/cmd/migrate"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository/legacyjson"
	"github.com/pkg/errors"
)

// migrateLegacyIfAny is invoked by Init() after AutoMigrate.
// Algorithm:
//  1. If _meta.migrated_from is non-empty, skip (already migrated or "(fresh install)" marker).
//  2. Locate the legacy file based on Config.Format (or auto-sniff json then yaml).
//  3. If no legacy file, write _meta.migrated_from = META_FRESH_INSTALL and return.
//  4. Read legacy file via legacyjson.NewReader.
//  5. Copy all entity types via cmd/migrate.MigrateBetween (runs its own tx on SQLite).
//  6. Write _meta.migrated_from = "<legacy filename>@<ISO8601>" and _meta.schema_version.
//  7. Rename legacy file with .migrated.<ts> suffix (rename failure is Warn only, non-fatal).
func (sp *SQLitePersistent) migrateLegacyIfAny() error {
	cur, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
	if err != nil {
		return err
	}
	if cur != "" {
		return nil
	}

	legacyPath := sp.detectLegacyPath()
	if legacyPath == "" {
		return writeMeta(sp.Client, METAKEY_MIGRATED_FROM, META_FRESH_INSTALL)
	}

	// 崩溃恢复：上一次 runLegacyMigration 可能已经提交了数据但在 writeMeta 之前死掉
	// （MigrateBetween 的 tx 与 _meta 写是两次独立提交）。重新启动时若 tbl_cluster 已有行，
	// 直接补上 _meta 标记，避免重跑迁移碰到 UNIQUE 冲突无法启动。
	var clusterCount int64
	if err := sp.Client.Model(&TblCluster{}).Count(&clusterCount).Error; err == nil && clusterCount > 0 {
		stamp := fmt.Sprintf("%s@%s (recovered)", filepath.Base(legacyPath), time.Now().UTC().Format(time.RFC3339))
		if err := writeMeta(sp.Client, METAKEY_MIGRATED_FROM, stamp); err != nil {
			return errors.Wrap(err, "finalize recovered migration meta")
		}
		log.Logger.Warnf("local persistent: prior migration left data committed without meta marker; finalized as %q", stamp)
		return nil
	}

	return sp.runLegacyMigration(legacyPath)
}

// detectLegacyPath returns the absolute path of the legacy JSON / YAML file to import.
// If Config.Format is set, only that format is tried. Otherwise json then yaml is tried.
func (sp *SQLitePersistent) detectLegacyPath() string {
	var candidates []string
	switch sp.Config.Format {
	case "json", ".json":
		candidates = []string{sp.Config.LegacyJSONPath()}
	case "yaml", ".yaml":
		candidates = []string{sp.Config.LegacyYAMLPath()}
	default:
		candidates = []string{sp.Config.LegacyJSONPath(), sp.Config.LegacyYAMLPath()}
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

func (sp *SQLitePersistent) runLegacyMigration(legacyPath string) error {
	fmtStr := formatFromPath(legacyPath)
	legacyCfg := legacyjson.LocalConfig{
		Format:     fmtStr,
		ConfigDir:  filepath.Dir(legacyPath),
		ConfigFile: trimExt(filepath.Base(legacyPath)),
	}
	src, err := legacyjson.NewReader(legacyCfg)
	if err != nil {
		return errors.Wrapf(err, "open legacy file %s", legacyPath)
	}

	// MigrateBetween manages its own Begin/Commit on dst (sp).
	if err := migrate.MigrateBetween(src, sp); err != nil {
		return errors.Wrap(err, "migrate")
	}

	// Write meta keys in a small follow-up write after the data migration commits.
	stamp := fmt.Sprintf("%s@%s", filepath.Base(legacyPath), time.Now().UTC().Format(time.RFC3339))
	if err := writeMeta(sp.Client, METAKEY_MIGRATED_FROM, stamp); err != nil {
		return errors.Wrap(err, "write meta migrated_from")
	}
	if err := writeMeta(sp.Client, METAKEY_SCHEMA_VERSION, SQLITE_SCHEMA_VERSION); err != nil {
		return errors.Wrap(err, "write meta schema_version")
	}

	if err := renameLegacyWithTimestamp(legacyPath); err != nil {
		log.Logger.Warnf("migrated successfully but failed to rename legacy file %s: %v", legacyPath, err)
	}
	log.Logger.Infof("local persistent: migrated from %s -> %s", legacyPath, sp.Config.DBPath())
	return nil
}

func formatFromPath(p string) string {
	switch filepath.Ext(p) {
	case ".yaml", ".yml":
		return ".yaml"
	default:
		return ".json"
	}
}

func trimExt(name string) string {
	return name[:len(name)-len(filepath.Ext(name))]
}

func renameLegacyWithTimestamp(legacyPath string) error {
	ts := time.Now().UTC().Format("20060102-150405")
	target := fmt.Sprintf("%s.migrated.%s", legacyPath, ts)
	if err := os.Rename(legacyPath, target); err != nil {
		return err
	}
	lastFile := legacyPath + ".last"
	if _, err := os.Stat(lastFile); err == nil {
		_ = os.Rename(lastFile, fmt.Sprintf("%s.migrated.%s", lastFile, ts))
	}
	return nil
}
