package enforce

import (
	"strings"

	"github.com/housepower/ckman/common"
)

const (
	POST   string = "POST"
	GET    string = "GET"
	PUT    string = "PUT"
	DELETE string = "DELETE"
)

type Policy struct {
	URL    string
	Method string
}

type Model struct {
	Admin     string
	UrlPrefix []string
}

type Enforcer struct {
	model   Model
	guest   []Policy
	orinary []Policy
}

var DefaultModel = Model{
	Admin: common.ADMIN,
	UrlPrefix: []string{
		"/api/v1", "/api/v2",
	},
}
var e *Enforcer

func init() {
	e = &Enforcer{
		model:   DefaultModel,
		guest:   GuestPolicies(),
		orinary: OrdinaryPolicies(),
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

	var policies []Policy
	if username == common.InternalOrdinaryName {
		policies = e.orinary
	} else {
		userinfo, err := common.GetUserInfo(username)
		if err != nil {
			return false
		}
		switch userinfo.Policy {
		case common.GUEST:
			policies = e.guest
		case common.ORDINARY:
			policies = e.orinary
		}
	}
	for _, policy := range policies {
		if e.Match(policy.URL, url) && policy.Method == method {
			return true
		}
	}
	return false
}
