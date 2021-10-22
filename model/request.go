package model

import (
	"github.com/housepower/ckman/log"
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

	err = json.Unmarshal(body, v)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	log.Logger.Debugf("[request] | %s | %s | %s \n%v", request.Host, request.Method, request.URL, string(data))
	return nil
}
