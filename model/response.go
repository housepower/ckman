package model

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/log"
	"net/http"
)

type ResponseBody struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func WrapMsg(c *gin.Context, code int, msg string, data interface{}) error {
	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/json; charset=utf-8")

	resp := ResponseBody{
		Code: code,
		Msg:  msg,
		Data: data,
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

	if code != SUCCESS {
		log.Logger.Errorf("%s %s return %d, %v", c.Request.Method, c.Request.RequestURI, code, data)
	}

	return nil
}
