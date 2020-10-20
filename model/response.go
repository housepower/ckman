package model

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/log"
	"net/http"
)

func WrapMsg(c *gin.Context, code int, msg string, data interface{}) {
	c.JSON(http.StatusOK, gin.H{
		"code": code,
		"msg":  msg,
		"data": data,
	})
	if code != SUCCESS {
		log.Logger.Errorf("%s %s return %d, %v", c.Request.Method, c.Request.RequestURI, code, data)
	}
}
