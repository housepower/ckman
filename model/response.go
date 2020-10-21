package model

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/log"
	"net/http"
)

func WrapMsg(c *gin.Context, code int, msg string, data interface{}) error {
	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/json; charset=utf-8")

	jsonBytes, err := json.Marshal(map[string]interface{}{
		"code": code,
		"msg":  msg,
		"data": data,
	})
	if err != nil {
		log.Logger.Errorf("%s %s marshal response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return err
	}

	_, err = c.Writer.Write(jsonBytes)
	if err != nil {
		log.Logger.Errorf("%s %s write response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return err
	}

	if code != SUCCESS {
		log.Logger.Errorf("%s %s return %d, %v", c.Request.Method, c.Request.RequestURI, code, data)
	}

	return nil
}
