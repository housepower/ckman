package model

import (
	jsoniter "github.com/json-iterator/go"
	"io/ioutil"
	"net/http"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func DecodeRequestBody(request *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, v)
}
