package sqlcli

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/housepower/ckman/log"
)

// RenderOptions 控制 table / vertical 渲染的截断和列宽。
type RenderOptions struct {
	TruncateAt int // 0 表示不截断；> 0 表示超过 N 个字符的字符串截断为 N-3+...
}

const DefaultTruncate = 60

// RenderTable 按 mysql 客户端的 ASCII table 样式渲染。
func RenderTable(w io.Writer, cols []string, rows [][]interface{}, opts RenderOptions) {
	if len(rows) == 0 {
		return
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
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, buildRow(cols, widths))
	fmt.Fprintln(w, sep)
	for _, row := range formatted {
		fmt.Fprintln(w, buildRow(row, widths))
	}
	fmt.Fprintln(w, sep)
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
func RenderVertical(w io.Writer, cols []string, rows [][]interface{}, opts RenderOptions) {
	truncate := opts.TruncateAt
	maxLabel := 0
	for _, c := range cols {
		if len(c) > maxLabel {
			maxLabel = len(c)
		}
	}
	for ri, row := range rows {
		fmt.Fprintf(w, "*************************** %d. row ***************************\n", ri+1)
		for ci, v := range row {
			fmt.Fprintf(w, "%*s: %s\n", maxLabel, cols[ci], formatCell(v, truncate))
		}
	}
}

// RenderJSON 输出 NDJSON：每行一个 JSON 对象。
// NULL → null；time.Time → RFC3339；[]byte → base64（json.Marshal 默认行为）。
func RenderJSON(w io.Writer, cols []string, rows [][]interface{}) {
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
		_ = enc.Encode(obj)
	}
}

// RenderCSV 输出 RFC4180 CSV，第一行为表头。
// NULL → 空字符串；time.Time → RFC3339；其他 → fmt.Sprintf("%v")。
func RenderCSV(w io.Writer, cols []string, rows [][]interface{}) {
	cw := csv.NewWriter(w)
	_ = cw.Write(cols)
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
		_ = cw.Write(record)
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		log.Logger.Warnf("csv flush: %v", err)
	}
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
