package clickhouse

import (
	"fmt"
	"testing"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/stretchr/testify/assert"
)

func TestGetLogicSchema(t *testing.T) {
	log.InitLoggerConsole()
	db, err := common.ConnectClickHouse("192.168.21.73", 9000, "default", "ck", "123456")
	assert.Nil(t, err)
	sqls, err := GetLogicSchema(db, "bench", "shanghai", true)
	assert.Nil(t, err)
	fmt.Printf("sqls:%#v\n", sqls)
}
