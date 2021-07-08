package model

import (
	json "github.com/bytedance/sonic"
	"io/ioutil"
	"net/http"
)


func DecodeRequestBody(request *http.Request, v interface{}) error {
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, v)
}
