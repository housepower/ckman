package model

import (
	"net/http"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/log"
)

type ResponseBody struct {
	RetCode int         `json:"retCode"`
	RetMsg  string      `json:"retMsg"`
	Entity  interface{} `json:"entity"`
}

func WrapMsg(c *gin.Context, retCode int, retMsg string, entity interface{}) error {
	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/json; charset=utf-8")

	if retCode != SUCCESS {
		log.Logger.Errorf("%s %s return %d, %v", c.Request.Method, c.Request.RequestURI, retCode, entity)
		if exception, ok := entity.(*clickhouse.Exception); ok {
			retCode = int(exception.Code)
			retMsg += ": " + exception.Message
		} else if exception, ok := entity.(error); ok {
			retMsg += ": " + exception.Error()
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
		return err
	}

	_, err = c.Writer.Write(jsonBytes)
	if err != nil {
		log.Logger.Errorf("%s %s write response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return err
	}

	return nil
}
