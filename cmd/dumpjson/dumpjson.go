package dumpjson

import (
	"fmt"
	"path/filepath"

	sqlitedriver "github.com/glebarez/sqlite"
	"github.com/housepower/ckman/cmd/migrate"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository/legacyjson"
	"github.com/housepower/ckman/repository/sqlite"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// DumpFromDB exports the SQLite database at dbPath into a legacy-format JSON
// file at outPath. ckman may be running while this runs — the SQLite file is
// opened in read-only mode (?mode=ro). We rely on WAL per-statement read
// isolation rather than wrapping the whole export in a transaction, because
// MigrateBetween manages its own dst-side tx and starting an src-side tx
// would conflict with downstream Begin calls. As a practical consequence,
// concurrent ckman writes that land between SELECTs may produce a slightly
// inconsistent snapshot (e.g., a backup_run committed mid-export may show up
// without its parent policy update). For strict consistency, stop ckman first.
func DumpFromDB(dbPath, outPath string) error {
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", dbPath)
	db, err := gorm.Open(sqlitedriver.Open(dsn), &gorm.Config{})
	if err != nil {
		return errors.Wrap(err, "open db ro")
	}

	src := sqlite.NewSQLitePersistent()
	src.AttachReadOnly(db)

	// Snapshot via deferred read tx. MigrateBetween will call src.Begin
	// internally too — but since we attached the same db twice, the second
	// Begin will fail with ErrTransActionBegin.
	// Instead, do not start a tx here; trust SQLite's WAL read isolation
	// for each individual SELECT. (Each SELECT sees the latest committed
	// state; cross-statement snapshot consistency is not guaranteed but
	// close enough for export semantics.)

	dst, err := legacyjson.NewWriter(legacyjson.LocalConfig{
		Format:     config.FORMAT_JSON,
		ConfigDir:  filepath.Dir(outPath),
		ConfigFile: trimExtBase(outPath),
	})
	if err != nil {
		return errors.Wrap(err, "open legacy writer")
	}

	if err := migrate.MigrateBetween(src, dst); err != nil {
		return errors.Wrap(err, "dump")
	}

	log.Logger.Infof("dumped %s -> %s", dbPath, outPath)
	return nil
}

// trimExtBase 只剥已知 legacy-format 扩展名（.json/.yaml/.yml）。
// 不剥任意"看似扩展名"的后缀 —— 否则 timestamp 形态（如 .20260516-032940）
// 会被误识为扩展名导致信息丢失。
func trimExtBase(p string) string {
	base := filepath.Base(p)
	ext := filepath.Ext(base)
	switch ext {
	case ".json", ".yaml", ".yml":
		return base[:len(base)-len(ext)]
	default:
		return base
	}
}
