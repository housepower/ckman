package sqlcli

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// RenderOptions 控制 table / vertical 渲染的截断和列宽。
type RenderOptions struct {
	TruncateAt int // 0 表示不截断；> 0 表示超过 N 个字符的字符串截断为 N-3+...
}

const DefaultTruncate = 60

// errWriter wraps an io.Writer and captures the first write error so
// callers can check once at the end rather than after every write.
type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) Write(p []byte) (int, error) {
	if ew.err != nil {
		return 0, ew.err
	}
	n, err := ew.w.Write(p)
	if err != nil {
		ew.err = err
	}
	return n, err
}

// RenderTable 按 mysql 客户端的 ASCII table 样式渲染。
func RenderTable(w io.Writer, cols []string, rows [][]interface{}, opts RenderOptions) error {
	if len(rows) == 0 {
		return nil
	}
	truncate := opts.TruncateAt
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	formatted := make([][]string, len(rows))
	for ri, row := range rows {
		formatted[ri] = make([]string, len(cols))
		for ci, v := range row {
			s := formatCell(v, truncate)
			formatted[ri][ci] = s
			if len(s) > widths[ci] {
				widths[ci] = len(s)
			}
		}
	}
	sep := buildSep(widths)
	ew := &errWriter{w: w}
	fmt.Fprintln(ew, sep)
	fmt.Fprintln(ew, buildRow(cols, widths))
	fmt.Fprintln(ew, sep)
	for _, row := range formatted {
		fmt.Fprintln(ew, buildRow(row, widths))
	}
	fmt.Fprintln(ew, sep)
	return ew.err
}

func buildSep(widths []int) string {
	var b strings.Builder
	b.WriteString("+")
	for _, w := range widths {
		b.WriteString(strings.Repeat("-", w+2))
		b.WriteString("+")
	}
	return b.String()
}

func buildRow(cells []string, widths []int) string {
	var b strings.Builder
	b.WriteString("|")
	for i, c := range cells {
		b.WriteString(" ")
		b.WriteString(c)
		b.WriteString(strings.Repeat(" ", widths[i]-len(c)))
		b.WriteString(" |")
	}
	return b.String()
}

// RenderVertical 按 mysql `\G` 样式：一行一行展示。
func RenderVertical(w io.Writer, cols []string, rows [][]interface{}, opts RenderOptions) error {
	truncate := opts.TruncateAt
	maxLabel := 0
	for _, c := range cols {
		if len(c) > maxLabel {
			maxLabel = len(c)
		}
	}
	ew := &errWriter{w: w}
	for ri, row := range rows {
		fmt.Fprintf(ew, "*************************** %d. row ***************************\n", ri+1)
		for ci, v := range row {
			fmt.Fprintf(ew, "%*s: %s\n", maxLabel, cols[ci], formatCell(v, truncate))
		}
	}
	return ew.err
}

// RenderJSON 输出 NDJSON：每行一个 JSON 对象。
// NULL → null；time.Time → RFC3339；[]byte → base64（json.Marshal 默认行为）。
func RenderJSON(w io.Writer, cols []string, rows [][]interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, row := range rows {
		obj := make(map[string]interface{}, len(cols))
		for ci, v := range row {
			if t, ok := v.(time.Time); ok {
				obj[cols[ci]] = t.UTC().Format(time.RFC3339)
			} else {
				obj[cols[ci]] = v
			}
		}
		if err := enc.Encode(obj); err != nil {
			return err
		}
	}
	return nil
}

// RenderCSV 输出 RFC4180 CSV，第一行为表头。
// NULL → 空字符串；time.Time → RFC3339；其他 → fmt.Sprintf("%v")。
func RenderCSV(w io.Writer, cols []string, rows [][]interface{}) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, len(row))
		for i, v := range row {
			switch x := v.(type) {
			case nil:
				record[i] = ""
			case time.Time:
				record[i] = x.UTC().Format(time.RFC3339)
			case []byte:
				record[i] = string(x)
			case string:
				record[i] = x
			default:
				record[i] = fmt.Sprintf("%v", x)
			}
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// formatCell 把任意类型转字符串，含 NULL 渲染、时间格式、可选截断。
func formatCell(v interface{}, truncateAt int) string {
	var s string
	switch x := v.(type) {
	case nil:
		return "(NULL)"
	case time.Time:
		s = x.Format("2006-01-02 15:04:05")
	case []byte:
		s = string(x)
	case string:
		s = x
	default:
		s = fmt.Sprintf("%v", x)
	}
	if truncateAt > 0 && len(s) > truncateAt {
		return s[:truncateAt-3] + "..."
	}
	return s
}
