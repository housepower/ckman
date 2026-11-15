package controller

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"strconv"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	diagnosisservice "github.com/housepower/ckman/service/diagnosis"
	ckagent "github.com/housepower/ckman/service/diagnosis/agent"
	novaagent "github.com/housepower/ckman/service/diagnosis/agent/nova"
)

type DiagnosisController struct {
	Controller
	orchestrator *diagnosisservice.Orchestrator
}

func NewDiagnosisController(wrapfunc Wrapfunc) *DiagnosisController {
	instance := fmt.Sprintf("%s:%d", config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)
	return &DiagnosisController{Controller: Controller{wrapfunc: wrapfunc}, orchestrator: diagnosisservice.NewOrchestrator(repository.Ps, instance, 2)}
}

func (ctl *DiagnosisController) Checks(c *gin.Context) {
	ctl.wrapfunc(c, model.E_SUCCESS, gin.H{"checks": diagnose.ListChecks(), "profiles": diagnosisservice.ListProfiles(), "llm_enabled": config.GlobalConfig.LLM.Enabled})
}

func (ctl *DiagnosisController) CreateRun(c *gin.Context) {
	var req diagnosisservice.StartRequest
	decoder := stdjson.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	run, err := ctl.orchestrator.StartRun(c.Param("cluster"), c.GetString("username"), req)
	if err != nil {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	ctl.wrapfunc(c, model.E_SUCCESS, gin.H{"run_id": run.RunID, "status": run.Status})
}

func (ctl *DiagnosisController) ListRuns(c *gin.Context) {
	limit, err := strconv.Atoi(defaultString(c.Query("limit"), "20"))
	if err != nil || limit < 1 || limit > 100 {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, "invalid limit")
		return
	}
	offset, err := strconv.Atoi(defaultString(c.Query("offset"), "0"))
	if err != nil || offset < 0 {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, "invalid offset")
		return
	}
	q := model.DiagnosisRunQuery{ClusterName: c.Param("cluster"), Status: c.Query("status"), Scene: c.Query("scene"), Limit: limit, Offset: offset}
	if !diagnosisAdmin(c.GetString("username")) {
		q.RequestedBy = c.GetString("username")
	}
	runs, err := repository.Ps.ListDiagnosisRuns(q)
	if err != nil {
		ctl.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	for i := range runs {
		runs[i].ReportJSON = ""
		runs[i].ErrorMessage = ""
	}
	ctl.wrapfunc(c, model.E_SUCCESS, runs)
}

func (ctl *DiagnosisController) GetRun(c *gin.Context) {
	run, ok := ctl.authorizedRun(c)
	if !ok {
		return
	}
	c.Set("skip_response_log", true)
	var report stdjson.RawMessage
	if run.ReportJSON != "" {
		report = stdjson.RawMessage(run.ReportJSON)
	}
	run.ReportJSON = ""
	ctl.wrapfunc(c, model.E_SUCCESS, gin.H{"run": run, "report": report})
}

func (ctl *DiagnosisController) CancelRun(c *gin.Context) {
	run, ok := ctl.authorizedRun(c)
	if !ok {
		return
	}
	canceled, err := ctl.orchestrator.Cancel(run)
	if err != nil {
		ctl.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	if !canceled {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, "run is not cancelable")
		return
	}
	ctl.wrapfunc(c, model.E_SUCCESS, gin.H{"run_id": run.RunID, "status": model.DiagnosisStatusCanceling})
}

func (ctl *DiagnosisController) Explain(c *gin.Context) {
	run, ok := ctl.authorizedRun(c)
	if !ok {
		return
	}
	if run.ReportJSON == "" {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, "diagnosis report is not ready")
		return
	}
	report, err := diagnosisservice.PrepareLLMReport([]byte(run.ReportJSON))
	if err != nil {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	engine, err := novaagent.New(config.GlobalConfig.LLM)
	if err != nil {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache, no-transform")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	writeEvent := func(event ckagent.Event) {
		raw, marshalErr := stdjson.Marshal(event)
		if marshalErr != nil {
			return
		}
		_, _ = fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event.Type, raw)
		c.Writer.Flush()
	}
	_ = engine.Explain(c.Request.Context(), ckagent.ExplainRequest{Report: report}, writeEvent)
}

func (ctl *DiagnosisController) authorizedRun(c *gin.Context) (model.DiagnosisRun, bool) {
	run, err := repository.Ps.GetDiagnosisRun(c.Param("runId"))
	if err != nil || run.ClusterName != c.Param("cluster") {
		ctl.wrapfunc(c, model.E_DATA_SELECT_FAILED, repository.ErrRecordNotFound)
		return run, false
	}
	username := c.GetString("username")
	if run.RequestedBy != username && !diagnosisAdmin(username) {
		ctl.wrapfunc(c, model.E_DATA_SELECT_FAILED, repository.ErrRecordNotFound)
		return run, false
	}
	return run, true
}

func diagnosisAdmin(username string) bool {
	if username == common.DefaultAdminName {
		return true
	}
	user, err := repository.Ps.GetUserByName(username)
	return err == nil && user.Enabled && user.Policy == common.ADMIN
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
