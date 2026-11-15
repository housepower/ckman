package diagnosis

import (
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxExceptionRunes = 512
	maxLLMReportBytes = 32 << 10
)

// SanitizeText removes control characters and bounds untrusted ClickHouse text.
func SanitizeText(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	var b strings.Builder
	count := 0
	for len(value) > 0 && count < maxRunes {
		r, size := utf8.DecodeRuneInString(value)
		value = value[size:]
		if r == utf8.RuneError && size == 1 {
			continue
		}
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			continue
		}
		b.WriteRune(r)
		count++
	}
	return b.String()
}

// PrepareLLMReport produces a bounded, deterministic data block. If the
// normalized report is still too large it keeps only the report envelope and
// explicit truncation metadata; raw exception/command fields never survive.
func PrepareLLMReport(raw []byte) ([]byte, error) {
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	redactJSON(value)
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(normalized) <= maxLLMReportBytes {
		return normalized, nil
	}
	summary := map[string]interface{}{"truncated": true, "original_bytes": len(normalized)}
	if root, ok := value.(map[string]interface{}); ok {
		for _, key := range []string{"scene", "execution", "outcome", "severity", "reasons", "started_at", "finished_at"} {
			if v, exists := root[key]; exists {
				summary[key] = v
			}
		}
		if checks, ok := root["checks"].([]interface{}); ok {
			summary["checks_total"] = len(checks)
		}
	}
	return json.Marshal(summary)
}

func redactJSON(value interface{}) {
	switch current := value.(type) {
	case map[string]interface{}:
		for key, child := range current {
			switch strings.ToLower(key) {
			case "last_exception", "command", "stack_trace", "stacktrace", "password", "secret", "api_key":
				current[key] = "[redacted]"
			case "query":
				if text, ok := child.(string); ok {
					current[key] = redactSQLLiterals(SanitizeText(text, maxExceptionRunes))
				}
			default:
				if text, ok := child.(string); ok {
					current[key] = SanitizeText(text, maxExceptionRunes)
				} else {
					redactJSON(child)
				}
			}
		}
	case []interface{}:
		for _, child := range current {
			redactJSON(child)
		}
	}
}

// redactSQLLiterals preserves the query shape needed for diagnosis while
// removing string literal contents that may carry tenant data or credentials.
func redactSQLLiterals(query string) string {
	var b strings.Builder
	for i := 0; i < len(query); {
		if query[i] != '\'' {
			b.WriteByte(query[i])
			i++
			continue
		}
		b.WriteString("'?'")
		i++
		for i < len(query) {
			if query[i] == '\\' && i+1 < len(query) {
				i += 2
				continue
			}
			if query[i] == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					i += 2
					continue
				}
				i++
				break
			}
			i++
		}
	}
	return b.String()
}
