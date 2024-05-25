package common

import (
	"testing"

	"github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/assert"
)

func TestJwt(t *testing.T) {
	expect := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjExMzYxNjAyNDUsIm5hbWUiOiJja21hbiIsImNsaWVudElwIjoiMTcyLjE2LjE0NC4xIn0.DC0kqo8YYyaFRuR4jfRVUwiDBTJpzAw0cOLe2R7yhLE"
	j := NewJWT()
	claims := CustomClaims{
		StandardClaims: jwt.StandardClaims{
			IssuedAt: 1136160245,
		},
		Name:     DefaultAdminName,
		ClientIP: "172.16.144.1",
	}
	token, err := j.CreateToken(claims)
	assert.Nil(t, err)
	assert.Equal(t, expect, token)
	claims2, code := j.ParserToken(expect)
	assert.Equal(t, "0000", code)
	assert.Equal(t, claims.ClientIP, claims2.ClientIP)
}
