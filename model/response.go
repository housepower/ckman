package model

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
	"net/http"
)

type ResponseBody struct {
	RetCode string      `json:"retCode"`
	RetMsg  string      `json:"retMsg"`
	Entity  interface{} `json:"entity"`
}

func WrapMsg(c *gin.Context, retCode string, entity interface{}) {
	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/json; charset=utf-8")

	retMsg := GetMsg(c, retCode)
	if retCode != SUCCESS {
		log.Logger.Errorf("%s %s return %s, %v", c.Request.Method, c.Request.RequestURI, retCode, entity)
		if err, ok := entity.(error); ok {
			var exception *clickhouse.Exception
			if errors.As(err, &exception) {
				retCode = fmt.Sprintf("%04d", exception.Code)
				retMsg += ": " + exception.Message
			} else {
				retMsg += ": " + err.Error()
			}
		} else if s, ok := entity.(string); ok {
			retMsg += ": " + s
		}
		entity = nil
	}

	resp := ResponseBody{
		RetCode: retCode,
		RetMsg:  retMsg,
		Entity:  entity,
	}
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		log.Logger.Errorf("%s %s marshal response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return
	}

	_, err = c.Writer.Write(jsonBytes)
	if err != nil {
		log.Logger.Errorf("%s %s write response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return
	}
}
