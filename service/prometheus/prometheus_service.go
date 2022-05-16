package prometheus

import (
	"context"
	"fmt"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
	"time"

	"github.com/housepower/ckman/model"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	m "github.com/prometheus/common/model"
)

type PrometheusService struct {
	Host    string
	Timeout int
	times   int
}

func NewPrometheusService(host string, timeout int) *PrometheusService {
	p := &PrometheusService{
		Host:    host,
		Timeout: timeout,
	}
	return p
}

func (p *PrometheusService) QueryMetric(params *model.MetricQueryReq) (m.Value, error) {
	p.times++

	client, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", p.Host),
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	v1api := v1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.Timeout)*time.Second)
	defer cancel()

	result, _, err := v1api.Query(ctx, params.Metric, time.Unix(params.Time, 0))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return result, nil
}

func (p *PrometheusService) QueryRangeMetric(params *model.MetricQueryRangeReq) (m.Value, error) {
	p.times++

	client, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", p.Host),
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	v1api := v1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.Timeout)*time.Second)
	defer cancel()

	r := v1.Range{
		Start: time.Unix(params.Start, 0),
		End:   time.Unix(params.End, 0),
		Step:  time.Duration(params.Step) * time.Second,
	}
	result, _, err := v1api.QueryRange(ctx, params.Metric, r)
	if err != nil {
		log.Logger.Errorf("get query range failed: %v", err)
		return nil, nil
	}

	return result, nil
}
