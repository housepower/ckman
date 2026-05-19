package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoteOverridePath(t *testing.T) {
	cases := []struct {
		name     string
		needSudo bool
		cwd      string
		want     string
	}{
		{"sudo path", true, "/home/eoi/clickhouse", "/etc/clickhouse-server/config.d/node_override.xml"},
		{"non-sudo path", false, "/home/eoi/clickhouse", "/home/eoi/clickhouse/etc/clickhouse-server/config.d/node_override.xml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RemoteOverridePath(tc.needSudo, tc.cwd)
			assert.Equal(t, tc.want, got)
		})
	}
}
