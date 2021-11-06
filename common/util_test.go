package common

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertDisk(t *testing.T) {
	assert.Equal(t, "388.00B", ConvertDisk(388))
	assert.Equal(t, "48.21KB", ConvertDisk(49367))
	assert.Equal(t, "231.99MB", ConvertDisk(243255465))
	assert.Equal(t, "33.73GB", ConvertDisk(36212124557))
	assert.Equal(t, "1.23TB", ConvertDisk(1352446565432))
	assert.Equal(t, "29.60PB", ConvertDisk(33323244132400112))
}

func TestMd5CheckSum(t *testing.T) {
	fmt.Println(Md5CheckSum("[abc]select * from system.tables\n"))
}