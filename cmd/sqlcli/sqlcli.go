package sqlcli

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// Options 是 ckmanctl sql 子命令的所有运行时参数。
type Options struct {
	Policy      string                 // persistent_policy（local / mysql / postgres / dm8）
	ConfMap     map[string]interface{} // persistent_config[policy] 反序列化结果
	Query       string                 // 非空 ⇒ 单次模式
	Format      string                 // "table" / "json" / "csv"，默认 table
	Vertical    bool                   // -E
	NoTruncate  bool                   // -N
	Out         io.Writer              // 默认 os.Stdout
	ErrOut      io.Writer              // 默认 os.Stderr
	HistoryFile string                 // 默认 $HOME/.ckman_sql_history
}

func (o *Options) normalize() {
	if o.Out == nil {
		o.Out = os.Stdout
	}
	if o.ErrOut == nil {
		o.ErrOut = os.Stderr
	}
	if o.Format == "" {
		o.Format = "table"
	}
	if o.HistoryFile == "" {
		if home := os.Getenv("HOME"); home != "" {
			o.HistoryFile = filepath.Join(home, ".ckman_sql_history")
		}
	}
}

// Run 是 ckmanctl sql 子命令的统一入口。
// Query 非空时进入单次模式后退出；否则进 REPL。
func Run(opts Options) error {
	opts.normalize()
	if opts.Query != "" {
		return RunSingleShot(opts)
	}
	return RunREPL(opts)
}

// RunSingleShot 执行一条 SQL，输出后退出。错误返回 non-nil（ckmanctl main 转 exit code 1）。
func RunSingleShot(opts Options) error {
	opts.normalize()
	db, backend, err := OpenDB(opts.Policy, opts.ConfMap)
	if err != nil {
		return errors.Wrap(err, "open db")
	}
	defer closeGorm(db)
	return executeOne(opts, db, backend, opts.Query)
}

// RunREPL 启动交互式 prompt，每行一条 SQL。EOF / exit / quit 退出。
// 单条 SQL 错误不退出，让用户改后重试。
func RunREPL(opts Options) error {
	opts.normalize()
	db, backend, err := OpenDB(opts.Policy, opts.ConfMap)
	if err != nil {
		return errors.Wrap(err, "open db")
	}
	defer closeGorm(db)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "ckman> ",
		HistoryFile:     opts.HistoryFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		HistoryLimit:    1000,
	})
	if err != nil {
		return errors.Wrap(err, "init readline")
	}
	defer rl.Close()

	fmt.Fprintf(opts.Out, "Connected to %s backend. Type 'exit' or Ctrl-D to quit.\n", backend)
	for {
		line, rerr := rl.Readline()
		if rerr == readline.ErrInterrupt {
			if line == "" {
				return nil
			}
			continue
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return errors.Wrap(rerr, "readline")
		}
		line = strings.TrimSpace(line)
		line = strings.TrimSuffix(line, ";")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if lower == "exit" || lower == "quit" {
			return nil
		}
		if err := executeOne(opts, db, backend, line); err != nil {
			fmt.Fprintf(opts.ErrOut, "ERROR: %v\n", err)
		}
	}
}

func executeOne(opts Options, db *gorm.DB, backend Backend, raw string) error {
	sqlText := Translate(raw, backend)
	start := time.Now()
	if isReadStatement(sqlText) {
		return runReadQuery(opts, db, sqlText, start)
	}
	return runWriteQuery(opts, db, sqlText, start)
}

func runReadQuery(opts Options, db *gorm.DB, sqlText string, start time.Time) error {
	rows, err := db.Raw(sqlText).Rows()
	if err != nil {
		return errors.Wrap(err, "query")
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return errors.Wrap(err, "columns")
	}
	var result [][]interface{}
	for rows.Next() {
		scanTargets := make([]interface{}, len(cols))
		scanValues := make([]sql.RawBytes, len(cols))
		for i := range scanTargets {
			scanTargets[i] = &scanValues[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return errors.Wrap(err, "scan")
		}
		row := make([]interface{}, len(cols))
		for i, raw := range scanValues {
			if raw == nil {
				row[i] = nil
			} else {
				row[i] = string(raw)
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows iteration")
	}
	dur := time.Since(start)
	renderRows(opts, cols, result)
	summary := opts.Out
	if opts.Format == "json" || opts.Format == "csv" {
		summary = opts.ErrOut
	}
	if len(result) == 0 {
		fmt.Fprintf(summary, "Empty set (%s)\n", formatDuration(dur))
	} else {
		fmt.Fprintf(summary, "%d row%s in set (%s)\n", len(result), plural(len(result)), formatDuration(dur))
	}
	return nil
}

func runWriteQuery(opts Options, db *gorm.DB, sqlText string, start time.Time) error {
	res := db.Exec(sqlText)
	if res.Error != nil {
		return errors.Wrap(res.Error, "exec")
	}
	dur := time.Since(start)
	fmt.Fprintf(opts.Out, "%d row%s affected (%s)\n", res.RowsAffected, plural(int(res.RowsAffected)), formatDuration(dur))
	return nil
}

func renderRows(opts Options, cols []string, rows [][]interface{}) {
	if len(rows) == 0 && opts.Format == "table" {
		return
	}
	truncate := DefaultTruncate
	if opts.NoTruncate {
		truncate = 0
	}
	switch opts.Format {
	case "json":
		RenderJSON(opts.Out, cols, rows)
	case "csv":
		RenderCSV(opts.Out, cols, rows)
	default:
		if opts.Vertical {
			RenderVertical(opts.Out, cols, rows, RenderOptions{TruncateAt: truncate})
		} else {
			RenderTable(opts.Out, cols, rows, RenderOptions{TruncateAt: truncate})
		}
	}
}

// isReadStatement 按首关键字判定 SQL 是读还是写。
// 用于选择 db.Raw().Rows() 还是 db.Exec()。
func isReadStatement(sqlText string) bool {
	fields := strings.Fields(strings.TrimSpace(sqlText))
	if len(fields) == 0 {
		return false
	}
	head := strings.ToUpper(fields[0])
	switch head {
	case "SELECT", "PRAGMA", "EXPLAIN", "WITH", "SHOW", "DESC", "DESCRIBE":
		return true
	}
	return false
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.3fs", d.Seconds())
}

func closeGorm(db *gorm.DB) {
	sqlDB, err := db.DB()
	if err != nil {
		log.Logger.Warnf("get underlying sql.DB: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		log.Logger.Warnf("close db: %v", err)
	}
}
