package controller

import (
	"bytes"
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/prometheus"
	"github.com/pkg/errors"
)

const (
	TITLE_CLICKHOUSE_TABLE  string = "ClickHouse Table KPIs"
	TITLE_CLICKHOUSE_NODE   string = "ClickHouse Node KPIs"
	TITLE_ZOOKEEPER_METRICS string = "ZooKeeper KPIs"
)

type MetricController struct {
	Controller
	config *config.CKManConfig
}

func NewMetricController(config *config.CKManConfig, wrapfunc Wrapfunc) *MetricController {
	mc := &MetricController{}
	mc.config = config
	mc.wrapfunc = wrapfunc
	return mc
}

// @Summary 查询性能指标
// @Description 通过promQL查询性能指标
// @version 1.0
// @Security ApiKeyAuth
// @Tags metrics
// @Accept  json
// @Param metric query string true "metric name" default(ClickHouseMetrics_Read)
// @Param time query string true "metric time" default(1606290000)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"metric":{"__name__":"ClickHouseMetrics_Read","instance":"192.168.101.105:9363","job":"clickhouse_exporter"},"value":[1606290000,"2"]}]}"
// @Router /api/v2/metric/query/{clusterName} [get]
func (controller *MetricController) Query(c *gin.Context) {
	var params model.MetricQueryReq
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	promService := prometheus.NewPrometheusService(fmt.Sprintf("%s:%d", conf.PromHost, conf.PromPort), 10)

	params.Metric = c.Query("metric")
	time, err := strconv.ParseInt(c.Query("time"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	params.Time = time

	value, err := promService.QueryMetric(&params)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, value)
}

// @Summary 查询范围指标
// @Description 通过PromQL查询范围指标，可指定时间段
// @version 1.0
// @Security ApiKeyAuth
// @Tags metrics
// @Accept  json
// @Param metric query string true "metric name" default(ClickHouseMetrics_Read)
// @Param start query string true "start time" default(1606290000)
// @Param end query string true "end time" default(1606290120)
// @Param step query string true "step window" default(60)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"metric":{"__name__":"ClickHouseMetrics_Read","instance":"192.168.101.105:9363","job":"clickhouse_exporter"},"values":[[1606290000,"2"],[1606290060,"2"],[1606290120,"2"]]}]}"
// @Router /api/v2/metric/query-range/{clusterName} [get]
func (controller *MetricController) QueryRange(c *gin.Context) {
	var params model.MetricQueryRangeReq
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	promService := prometheus.NewPrometheusService(fmt.Sprintf("%s:%d", conf.PromHost, conf.PromPort), 10)

	params.Title = c.Query("title")
	var hosts []string
	if params.Title == TITLE_CLICKHOUSE_TABLE || params.Title == TITLE_CLICKHOUSE_NODE {
		hosts = conf.Hosts
	} else if params.Title == TITLE_ZOOKEEPER_METRICS {
		hosts = conf.ZkNodes
	} else {
		err := errors.Wrap(nil, fmt.Sprintf("title %s invalid", params.Title))
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	templHosts := "(" + strings.Join(hosts, "|") + "):.*"

	metric := c.Query("metric")
	replace := make(map[string]interface{})
	replace["hosts"] = templHosts
	t, err := template.New("T1").Parse(metric)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, replace)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	params.Metric = buf.String()
	log.Logger.Debugf("metric: %s", params.Metric)
	start, err := strconv.ParseInt(c.Query("start"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	end, err := strconv.ParseInt(c.Query("end"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	step, err := strconv.ParseInt(c.Query("step"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	params.Start = start
	params.End = end
	params.Step = step

	value, err := promService.QueryRangeMetric(&params)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, value)
}

func (controller *MetricController) QueryMetric(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	title := c.Query("title")
	start, err := strconv.ParseInt(c.Query("start"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	end, err := strconv.ParseInt(c.Query("end"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	step, err := strconv.ParseInt(c.Query("step"), 10, 64)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	m, ok := model.MetricMap[title]
	if !ok {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Errorf("title %s not found", title))
	}
	query := fmt.Sprintf(`SELECT toStartOfInterval(event_time, INTERVAL %d SECOND) AS t, %s
	FROM %s
	WHERE event_date >= %d AND event_time >= %d
	AND event_date <= %d AND event_time <= %d 
	GROUP BY t
	ORDER BY t WITH FILL STEP %d`, step, m.Field, m.Table, start, start, end, end, step)
	log.Logger.Debugf("query: %v", query)
	var rsps []model.MetricRsp
	for _, host := range conf.Hosts {
		rsp := model.MetricRsp{
			Metric: model.Metric{
				Job:      "ckman",
				Name:     m.Field,
				Instance: host,
			},
		}
		tmp := conf
		tmp.Hosts = []string{host}
		s := clickhouse.NewCkService(&tmp)
		err = s.InitCkService()
		if err != nil {
			return
		}
		data, err := s.QueryInfo(query)
		if err != nil {
			return
		}
		rsp.Value = data[1:]
		rsps = append(rsps, rsp)
	}
	controller.wrapfunc(c, model.E_SUCCESS, rsps)
}
