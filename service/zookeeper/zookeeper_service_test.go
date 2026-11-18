package zookeeper

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fakeZkMetric(responses map[string]string, errs map[string]error) func(string, int, string) ([]byte, error) {
	return func(host string, port int, metric string) ([]byte, error) {
		if err, ok := errs[host]; ok {
			return nil, err
		}
		return []byte(responses[host]), nil
	}
}

// A single-node ZooKeeper reports its state as "standalone", not "leader".
func TestFindUsableNodeAcceptsStandalone(t *testing.T) {
	probe := fakeZkMetric(map[string]string{
		"10.0.0.1": `{"host":"10.0.0.1","server_state":"standalone"}`,
	}, nil)

	node, err := findUsableNode([]string{"10.0.0.1"}, 2181, probe)

	assert.NoError(t, err)
	assert.Equal(t, "10.0.0.1", node)
}

func TestFindUsableNodeAcceptLeader(t *testing.T) {
	probe := fakeZkMetric(map[string]string{
		"10.0.0.1": `{"host":"10.0.0.1","server_state":"follower"}`,
		"10.0.0.2": `{"host":"10.0.0.2","server_state":"leader"}`,
	}, nil)

	node, err := findUsableNode([]string{"10.0.0.1", "10.0.0.2"}, 2181, probe)

	assert.NoError(t, err)
	assert.Equal(t, "10.0.0.2", node)
}

func TestFindUsableNodeReportsFailureDetails(t *testing.T) {
	probe := fakeZkMetric(
		map[string]string{
			"10.0.0.1": `{"host":"10.0.0.1","server_state":"follower"}`,
			"10.0.0.2": "mntr is not executed because it is not in the whitelist.",
		},
		map[string]error{
			"10.0.0.3": errors.New("connection refused"),
		},
	)

	_, err := findUsableNode([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, 2181, probe)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no leader zookeeper node")
	assert.Contains(t, err.Error(), `10.0.0.1: server state is "follower"`)
	assert.Contains(t, err.Error(), "10.0.0.2")
	assert.Contains(t, err.Error(), "10.0.0.3: connection refused")
}
