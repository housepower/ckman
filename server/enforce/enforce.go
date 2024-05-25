package enforce

import (
	"strings"
)

const (
	ADMIN string = "ckman"
	GUEST string = "guest"

	POST   string = "POST"
	GET    string = "GET"
	PUT    string = "PUT"
	DELETE string = "DELETE"
)

type Policy struct {
	User   string
	URL    string
	Method string
}

type Model struct {
	Admin     string
	UrlPrefix []string
}

type Enforcer struct {
	model    Model
	policies []Policy
}

var DefaultModel = Model{
	Admin: ADMIN,
	UrlPrefix: []string{
		"/api/v1", "/api/v2",
	},
}
var e *Enforcer

func init() {
	e = &Enforcer{
		model: DefaultModel,
		policies: []Policy{
			{GUEST, "/ck/cluster", GET},
			{GUEST, "/ck/cluster/*", GET},
			{GUEST, "/ck/table/*", GET},
			{GUEST, "/ck/table/group_uniq_array/*", GET},
			{GUEST, "/ck/query/*", GET},
			{GUEST, "/ck/query_explain/*", GET},
			{GUEST, "/ck/query_history/*", GET},
			{GUEST, "/ck/table_lists/*", GET},
			{GUEST, "/ck/table_schema/*", GET},
			{GUEST, "/ck/get/*", GET},
			{GUEST, "/ck/partition/*", GET},
			{GUEST, "/ck/table_metric/*", GET},
			{GUEST, "/ck/table_merges/*", GET},
			{GUEST, "/ck/open_sessions/*", GET},
			{GUEST, "/ck/slow_sessions/*", GET},
			{GUEST, "/ck/ddl_queue/*", GET},
			{GUEST, "/ck/node/log/*", POST},
			{GUEST, "/ck/ping/*", POST},
			{GUEST, "/ck/config/*", GET},
			{GUEST, "/zk/status/*", GET},
			{GUEST, "/zk/replicated_table/*", GET},
			{GUEST, "/package", GET},
			{GUEST, "/metric/query/*", GET},
			{GUEST, "/metric/query_range/*", GET},
			{GUEST, "/version", GET},
			{GUEST, "/ui/schema", GET},
			{GUEST, "/task/*", GET},
			{GUEST, "/task/lists", GET},
			{GUEST, "/task/running", GET},
		},
	}
}

func (e *Enforcer) Match(url1, url2 string) bool {
	for _, prefix := range e.model.UrlPrefix {
		if !strings.HasPrefix(url2, prefix) {
			return false
		}
		url22 := strings.TrimPrefix(url2, prefix)
		url11 := strings.TrimSuffix(url1, "*")
		if strings.HasPrefix(url22, url11) {
			return true
		}
	}

	return false
}

func Enforce(username, url, method string) bool {
	if username == e.model.Admin {
		return true
	}

	for _, policy := range e.policies {
		if policy.User == username && e.Match(policy.URL, url) && policy.Method == method {
			return true
		}
	}
	return false
}
