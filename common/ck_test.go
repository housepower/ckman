package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, 0, CompareClickHouseVersion("22.3.3.44", "22.x"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.3.45", "22.x"))
}

func TestCkPasswd(t *testing.T) {
	passwd := "cyc2010"
	assert.Equal(t, "a40943925ca51a95de7d39bc8c31757207d53b5e7114e695c04db63b6868f3e1", CkPassword(passwd, SHA256_HEX))
	assert.Equal(t, "812b8fad11eb25a3cf4cc2c54ae10a4948a0c25b", CkPassword(passwd, DOUBLE_SHA1_HEX))
	assert.Equal(t, passwd, CkPassword(passwd, PLAINTEXT))

	assert.Equal(t, "a5e5af71ae6063f531f6d5e0982e7e3741f09e3876b24d0cb89f2820c34fc065", CkPassword("4ADUUA5O", SHA256_HEX))
	assert.Equal(t, "15fae939139a48b92e715609560e487298568076", CkPassword("FKP8OfUF", DOUBLE_SHA1_HEX))

}

func TestCkPasswdLabel(t *testing.T) {
	assert.Equal(t, "password_sha256_hex", CkPasswdLabel(SHA256_HEX))
	assert.Equal(t, "password_double_sha1_hex", CkPasswdLabel(DOUBLE_SHA1_HEX))
	assert.Equal(t, "password", CkPasswdLabel(PLAINTEXT))
	assert.Equal(t, "password", CkPasswdLabel(-1))
}
