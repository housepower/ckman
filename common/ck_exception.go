package common

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	OK                     int32 = 0
	BAD_ARGUMENTS          int32 = 36
	UNKNOWN_TABLE          int32 = 60
	REPLICA_ALREADY_EXISTS int32 = 253
	UNFINISHED             int32 = 341
)

func ClikHouseExceptionDecode(err error) error {
	var e *clickhouse.Exception
	// TCP protocol
	if errors.As(err, &e) {
		return e
	}
	// HTTP protocol
	if strings.HasPrefix(err.Error(), "clickhouse [execute]::") {
		r := regexp.MustCompile(`.*Code:\s+(\d+)\.\s+(.*)`)
		matchs := r.FindStringSubmatch(err.Error())
		if len(matchs) != 3 {
			return err
		}
		code, err2 := strconv.Atoi(matchs[1])
		if err2 != nil {
			return err
		}
		message := matchs[2]
		e = &clickhouse.Exception{
			Code:    int32(code),
			Message: message,
		}
		return e
	}

	return err
}

func ExceptionAS(err error, code int32) bool {
	var e *clickhouse.Exception
	err = ClikHouseExceptionDecode(err)
	if errors.As(err, &e) {
		return e.Code == code
	}
	return false
}
