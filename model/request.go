package model

import (
	"io"
	"net/http"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func DecodeRequestBody(request *http.Request, v interface{}) error {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, v)
}
