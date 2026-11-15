package diagnosis

import (
	"context"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

type connectFunc func(context.Context, string, string, model.ConnetOption) (*common.Conn, error)

// ckQuerier connects lazily so connection preparation is covered by the
// per-node and total diagnosis contexts. Connections come from CKMAN's shared
// pool and are therefore not closed by this adapter.
type ckQuerier struct {
	host    string
	options model.ConnetOption
	connect connectFunc
}

func (q *ckQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
	conn, err := q.connect(ctx, q.host, model.ClickHouseDefaultDB, q.options)
	if err != nil {
		return nil, err
	}
	return conn.QueryWithCtx(ctx, query, args...)
}

var _ diagnose.Querier = (*ckQuerier)(nil)
