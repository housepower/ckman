package model

import (
	"io"
	"net/http"

	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func DecodeRequestBody(request *http.Request, v interface{}) error {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return errors.Wrap(err, "")
	}

	err = json.Unmarshal(body, v)
	if err != nil {
		return errors.Wrap(err, "")
	}

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return errors.Wrap(err, "")
	}
	log.Logger.Debugf("[request] | %s | %s | %s \n%v", request.Host, request.Method, request.URL, string(data))
	return nil
}
