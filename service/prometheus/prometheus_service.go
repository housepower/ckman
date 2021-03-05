package prometheus

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	m "github.com/prometheus/common/model"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"time"
)

type PrometheusService struct {
	config *config.CKManPrometheusConfig
	times  int
	hosts  int
}

func NewPrometheusService(config *config.CKManPrometheusConfig) *PrometheusService {
	p := &PrometheusService{}
	p.config = config
	p.hosts = len(config.Hosts)
	return p
}

func (p *PrometheusService) QueryMetric(params *model.MetricQueryReq) (m.Value, error) {
	if p.hosts == 0 {
		return nil, fmt.Errorf("prometheus service unavailable")
	}
	p.times++

	client, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", p.config.Hosts[p.times%p.hosts]),
	})
	if err != nil {
		return nil, err
	}

	v1api := v1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.config.Timeout)*time.Second)
	defer cancel()

	result, _, err := v1api.Query(ctx, params.Metric, time.Unix(params.Time, 0))
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *PrometheusService) QueryRangeMetric(params *model.MetricQueryRangeReq) (m.Value, error) {
	if p.hosts == 0 {
		return nil, fmt.Errorf("prometheus service unavailable")
	}
	p.times++

	client, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", p.config.Hosts[p.times%p.hosts]),
	})
	if err != nil {
		return nil, err
	}

	v1api := v1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.config.Timeout)*time.Second)
	defer cancel()

	r := v1.Range{
		Start: time.Unix(params.Start, 0),
		End:   time.Unix(params.End, 0),
		Step:  time.Duration(params.Step) * time.Second,
	}
	result, _, err := v1api.QueryRange(ctx, params.Metric, r)
	if err != nil {
		return nil, err
	}

	return result, nil
}
