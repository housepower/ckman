package clickhouse

import (
	"context"
	"io"
	"reflect"
	"sync"

	v2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
)

// streamBatchRows 是每个 data 事件携带的行数。
// 太大则前端长时间看不到新数据,太小则事件开销占比升高。
const streamBatchRows = 100

// streamCancels 是在途流式查询的取消函数注册表:qid → cancel。
// 注册/注销由 controller 在请求生命周期内完成,CancelStream 供取消接口调用。
var streamCancels sync.Map

// RegisterStreamCancel 注册流式查询的取消函数。重复注册同一 qid 会覆盖旧的。
func RegisterStreamCancel(qid string, cancel context.CancelFunc) {
	streamCancels.Store(qid, cancel)
}

// UnregisterStreamCancel 注销流式查询。请求结束(成功/失败/取消)时必须调用。
func UnregisterStreamCancel(qid string) {
	streamCancels.Delete(qid)
}

// CancelStream 取消一个在途流式查询。返回 false 表示 qid 不在注册表中
// (查询已结束或从未存在)。
func CancelStream(qid string) bool {
	if cancel, ok := streamCancels.Load(qid); ok {
		cancel.(context.CancelFunc)()
		return true
	}
	return false
}

// StreamEmit 是流式事件的发出回调:event 为事件类型(meta/progress/data/done),
// payload 为可 JSON 序列化的载荷。返回非 nil error 表示客户端已不可写
// (通常是断连),查询应当中止。
type StreamEmit func(event string, payload interface{}) error

// QueryInfoStream 流式执行查询,结果通过 emit 逐批吐出,不在内存中收集。
//
// 与 QueryInfo 的区别:QueryInfo 把结果全量读进 [][]interface{} 后一次性返回
// (供指标采集等需要完整结果的调用方);本函数面向 ad-hoc 查询页面,逐批 emit。
//
// 事件序列:meta(列名) → progress*(仅 native 协议) → data*(行批次) → done。
// qid 会作为 ClickHouse query_id 下发,同时应已用 RegisterStreamCancel 注册,
// 可通过 CancelStream(qid) 或 ctx 取消(前端断连)终止查询。
func (ck *CkService) QueryInfoStream(ctx context.Context, query, qid string, emit StreamEmit) error {
	if ck.Conn == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	log.Logger.Debugf("[%s]%s", qid, query)

	qopts := []v2.QueryOption{v2.WithQueryID(qid)}
	if ck.Config.Protocol != v2.HTTP.String() {
		// HTTP 协议下服务端不发送 Progress 包,注册了也收不到,只在 native 下挂。
		qopts = append(qopts, v2.WithProgress(func(p *v2.Progress) {
			// 进度事件发不出去不致命(客户端可能正忙),忽略 emit 错误。
			_ = emit("progress", map[string]interface{}{
				"rows":       p.Rows,
				"bytes":      p.Bytes,
				"total_rows": p.TotalRows,
				"elapsed_ns": p.Elapsed.Nanoseconds(),
			})
		}))
	}
	ctx = v2.Context(ctx, qopts...)

	rows, err := ck.Conn.QueryWithCtx(ctx, query)
	if err != nil {
		if err == io.EOF {
			// 与正常路径保持同一事件序列:meta → data → done,
			// 数据行只能走 data 事件,客户端无需为 EOF 特判。
			if err := emit("meta", map[string]interface{}{"qid": qid, "columns": []string{"result"}}); err != nil {
				return err
			}
			if err := emit("data", map[string]interface{}{"rows": [][]interface{}{{"OK"}}}); err != nil {
				return err
			}
			return emit("done", map[string]interface{}{"total_rows": 1})
		}
		return errors.Wrap(err, "")
	}
	defer rows.Close()

	cols, _ := rows.Columns()

	columnPointers := make([]interface{}, 0, len(cols))
	ctps, _ := rows.ColumnTypes()
	for _, ctp := range ctps {
		if ck.Config.Protocol == v2.HTTP.String() {
			columnPointers = append(columnPointers, reflect.New(ctp.ScanType()).Interface())
		} else {
			if ctp.ScanType().Kind() == reflect.Ptr {
				columnPointers = append(columnPointers, reflect.New(ctp.ScanType().Elem()).Interface())
			} else {
				columnPointers = append(columnPointers, reflect.New(ctp.ScanType()).Interface())
			}
		}
	}

	if err := emit("meta", map[string]interface{}{"qid": qid, "columns": cols}); err != nil {
		return err
	}

	var total uint64
	batch := make([][]interface{}, 0, streamBatchRows)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := emit("data", map[string]interface{}{"rows": batch}); err != nil {
			return err
		}
		batch = make([][]interface{}, 0, streamBatchRows)
		return nil
	}

	for rows.Next() {
		if err = rows.Scan(columnPointers...); err != nil {
			return errors.Wrap(err, "")
		}

		m := make([]interface{}, len(cols))
		for i := range columnPointers {
			val := reflect.ValueOf(columnPointers[i]).Elem()
			currentValue := val.Interface()
			if currentValue == nil {
				switch val.Kind() {
				case reflect.Slice:
					currentValue = reflect.MakeSlice(val.Type(), 0, 0).Interface()
				case reflect.Map:
					currentValue = reflect.MakeMap(val.Type()).Interface()
				case reflect.Ptr:
					elemType := val.Type().Elem()
					ptr := reflect.New(elemType)
					ptr.Elem().Set(reflect.Zero(elemType))
					currentValue = ptr.Interface()
				case reflect.Interface, reflect.Chan, reflect.Func:
				default:
					currentValue = reflect.Zero(val.Type()).Interface()
				}
			}
			m[i] = currentValue
			if val.CanSet() {
				val.SetZero()
			}
		}

		batch = append(batch, m)
		total++
		if len(batch) >= streamBatchRows {
			if err := flush(); err != nil {
				return err
			}
			// 取消在块边界生效:驱动层 ctx 取消后 Next() 会停,这里再查一次
			// 让"正在停止"更快落地。
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	if err := rows.Err(); err != nil && err != io.EOF {
		return errors.Wrap(err, "")
	}

	return emit("done", map[string]interface{}{"total_rows": total})
}
