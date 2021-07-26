package controller

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/prometheus"
	"github.com/pkg/errors"
	"html/template"
	"strconv"
	"strings"
)

const (
	TITLE_CLICKHOUSE_TABLE  string = "ClickHouse Table KPIs"
	TITLE_CLICKHOUSE_NODE   string = "ClickHouse Node KPIs"
	TITLE_ZOOKEEPER_METRICS string = "ZooKeeper KPIs"
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
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5050","retMsg":"get query metric failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"metric":{"__name__":"ClickHouseMetrics_Read","instance":"192.168.101.105:9363","job":"clickhouse_exporter"},"value":[1606290000,"2"]}]}"
// @Router /api/v1/metric/query [get]
func (m *MetricController) Query(c *gin.Context) {
	var params model.MetricQueryReq

	params.Metric = c.Query("metric")
	time, err := strconv.ParseInt(c.Query("time"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	params.Time = time

	value, err := m.promService.QueryMetric(&params)
	if err != nil {
		model.WrapMsg(c, model.QUERY_METRIC_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, value)
}

// @Summary Query Range
// @Description Query Range
// @version 1.0
// @Security ApiKeyAuth
// @Param metric query string true "metric name" default(ClickHouseMetrics_Read)
// @Param start query string true "start time" default(1606290000)
// @Param end query string true "end time" default(1606290120)
// @Param step query string true "step window" default(60)
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5051","retMsg":"get range-metric failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"metric":{"__name__":"ClickHouseMetrics_Read","instance":"192.168.101.105:9363","job":"clickhouse_exporter"},"values":[[1606290000,"2"],[1606290060,"2"],[1606290120,"2"]]}]}"
// @Router /api/v1/metric/query_range [get]
func (m *MetricController) QueryRange(c *gin.Context) {
	var params model.MetricQueryRangeReq
	clusterName := c.Param(ClickHouseClusterPath)
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	params.Title = c.Query("title")
	var hosts []string
	if params.Title == TITLE_CLICKHOUSE_TABLE || params.Title == TITLE_CLICKHOUSE_NODE {
		hosts = conf.Hosts
	} else if params.Title == TITLE_ZOOKEEPER_METRICS {
		hosts = conf.ZkNodes
	} else {
		err := errors.Wrap(nil, fmt.Sprintf("title %s invalid", params.Title))
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	templHosts := "(" + strings.Join(hosts, "|") + "):.*"

	metric := c.Query("metric")
	replace := make(map[string]interface{})
	replace["hosts"] = templHosts
	t, err := template.New("T1").Parse(metric)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, replace)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	params.Metric = buf.String()
	log.Logger.Debugf("metric: %s", params.Metric)
	start, err := strconv.ParseInt(c.Query("start"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	end, err := strconv.ParseInt(c.Query("end"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	step, err := strconv.ParseInt(c.Query("step"), 10, 64)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	params.Start = start
	params.End = end
	params.Step = step

	value, err := m.promService.QueryRangeMetric(&params)
	if err != nil {
		model.WrapMsg(c, model.QUERY_RANGE_METRIC_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, value)
}
