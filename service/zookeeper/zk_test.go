package zookeeper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetric(t *testing.T) {
	resp, err := ZkMetric("192.168.101.94", 2181, "mntr")
	assert.Nil(t, err)
	fmt.Println(string(resp))
}
