package controller

import "github.com/gin-gonic/gin"

type Wrapfunc func(c *gin.Context, retCode string, entity interface{})

type Controller struct {
	wrapfunc Wrapfunc
}
