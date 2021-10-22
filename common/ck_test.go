package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompareClickHouseVersion(t *testing.T) {
	assert.Equal(t, 1, CompareClickHouseVersion("21.9.3.45", "20.10.8.2403"))
	assert.Equal(t, 1, CompareClickHouseVersion("21.9.3.45", "21.7"))
	assert.Equal(t, 1, CompareClickHouseVersion("21.10.3.45", "21.9.8.2403"))
	assert.Equal(t, 1, CompareClickHouseVersion("21.9.3.1018", "20.9.3.45"))
	assert.Equal(t, 0, CompareClickHouseVersion("21.9.3.45", "21.9.x.x"))
	assert.Equal(t, 0, CompareClickHouseVersion("21.9.3.45", "21.9"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.x.x", "21.10.x.x"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.3.45", "21.9.3.2403"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.3", "21.10.8.2403"))
}
