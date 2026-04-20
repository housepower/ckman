package zookeeper

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetric(t *testing.T) {
	host := os.Getenv("CKMAN_TEST_ZK_HOST")
	if host == "" {
		t.Skip("set CKMAN_TEST_ZK_HOST to run zookeeper metric integration test")
	}
	port := 9181
	if rawPort := os.Getenv("CKMAN_TEST_ZK_PORT"); rawPort != "" {
		parsedPort, err := strconv.Atoi(rawPort)
		assert.Nil(t, err)
		port = parsedPort
	}

	resp, err := ZkMetric(host, port, "mntr")
	assert.Nil(t, err)
	fmt.Println(string(resp))
}
