package model

import (
	"bufio"
	jsoniter "github.com/json-iterator/go"
	"net/http"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func DecodeRequestBody(request *http.Request, v interface{}) error {
	body := make([]byte, request.ContentLength)

	r := bufio.NewReader(request.Body)
	if _, err := r.Read(body); err != nil {
		return err
	}

	return json.Unmarshal(body, v)
}
