package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/prometheus"
	"strconv"
)

type MetricController struct {
	config      *config.CKManConfig
	promService *prometheus.PrometheusService
}

func NewMetricController(config *config.CKManConfig, promService *prometheus.PrometheusService) *MetricController {
	ck := &MetricController{}
	ck.config = config
	ck.promService = promService
	return ck
}

// @Summary Query
// @Description Query
// @version 1.0
// @Security ApiKeyAuth
// @Param metric query string true "metric name" default(ClickHouseMetrics_Read)
// @Param time query string true "metric time" default(1606290000)
// @Failure 200 {string} json "{"code":400,"msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":5050,"msg":"get query metric failed","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":[{"metric":{"__name__":"ClickHouseMetrics_Read","instance":"192.168.101.105:9363","job":"clickhouse_exporter"},"value":[1606290000,"2"]}]}"
// @Router /api/v1/metric/query [get]
func (m *MetricController) Query(c *gin.Context) {
	var params model.MetricQueryReq

	params.Metric = c.Query("metric")
	time, err := strconv.ParseInt(c.Query("time"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}
	params.Time = time

	value, err := m.promService.QueryMetric(&params)
	if err != nil {
		model.WrapMsg(c, model.QUERY_METRIC_FAIL, model.GetMsg(c, model.QUERY_METRIC_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), value)
}

// @Summary Query Range
// @Description Query Range
// @version 1.0
// @Security ApiKeyAuth
// @Param metric query string true "metric name" default(ClickHouseMetrics_Read)
// @Param start query string true "start time" default(1606290000)
// @Param end query string true "end time" default(1606290120)
// @Param step query string true "step window" default(60)
// @Failure 200 {string} json "{"code":400,"msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":5051,"msg":"get range-metric failed","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":[{"metric":{"__name__":"ClickHouseMetrics_Read","instance":"192.168.101.105:9363","job":"clickhouse_exporter"},"values":[[1606290000,"2"],[1606290060,"2"],[1606290120,"2"]]}]}"
// @Router /api/v1/metric/query_range [get]
func (m *MetricController) QueryRange(c *gin.Context) {
	var params model.MetricQueryRangeReq

	params.Metric = c.Query("metric")
	start, err := strconv.ParseInt(c.Query("start"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}
	end, err := strconv.ParseInt(c.Query("end"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}
	step, err := strconv.ParseInt(c.Query("step"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}
	params.Start = start
	params.End = end
	params.Step = step

	value, err := m.promService.QueryRangeMetric(&params)
	if err != nil {
		model.WrapMsg(c, model.QUERY_RANGE_METRIC_FAIL, model.GetMsg(c, model.QUERY_RANGE_METRIC_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), value)
}
