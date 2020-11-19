package controller

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
	"gitlab.eoitek.net/EOI/ckman/service/prometheus"
	"strconv"
)

type MetricController struct {
	config      *config.CKManConfig
	ckService   *clickhouse.CkService
	promService *prometheus.PrometheusService
}

func NewMetricController(config *config.CKManConfig, ckService *clickhouse.CkService, promService *prometheus.PrometheusService) *MetricController {
	ck := &MetricController{}
	ck.config = config
	ck.ckService = ckService
	ck.promService = promService
	return ck
}

// @Summary 查询指标
// @Description 查询指标
// @version 1.0
// @Security ApiKeyAuth
// @Param metric query string true "metric name"
// @Param time query string true "metric time"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5050,"msg":"获取指标失败","data":""}"
// @Router /api/v1/metric/query [get]
func (m *MetricController) Query(c *gin.Context) {
	var params model.MetricQueryReq

	params.Metric = c.Query("metric")
	time, err := strconv.ParseInt(c.Query("time"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}
	params.Time = time

	value, err := m.promService.QueryMetric(&params)
	if err != nil {
		model.WrapMsg(c, model.QUERY_METRIC_FAIL, model.GetMsg(model.QUERY_METRIC_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), value)
}

// @Summary 查询指标范围
// @Description 查询指标范围
// @version 1.0
// @Security ApiKeyAuth
// @Param metric query string true "metric name"
// @Param start query string true "start time"
// @Param end query string true "end time"
// @Param step query string true "step window"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5051,"msg":"获取指标范围失败","data":""}"
// @Router /api/v1/metric/query_range [get]
func (m *MetricController) QueryRange(c *gin.Context) {
	var params model.MetricQueryRangeReq

	params.Metric = c.Query("metric")
	start, err := strconv.ParseInt(c.Query("start"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}
	end, err := strconv.ParseInt(c.Query("end"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}
	step, err := strconv.ParseInt(c.Query("step"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}
	params.Start = start
	params.End = end
	params.Step = step

	value, err := m.promService.QueryRangeMetric(&params)
	if err != nil {
		model.WrapMsg(c, model.QUERY_RANGE_METRIC_FAIL, model.GetMsg(model.QUERY_RANGE_METRIC_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), value)
}
