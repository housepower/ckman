package common

import (
	"github.com/dgrijalva/jwt-go"
	"github.com/housepower/ckman/model"
)

var (
	DefaultSigningKey = "change me"
)

type JWT struct {
	SigningKey []byte
}

func NewJWT() *JWT {
	return &JWT{
		[]byte(DefaultSigningKey),
	}
}

type CustomClaims struct {
	jwt.StandardClaims
	Name     string `json:"name"`
	ClientIP string `json:"clientIp"`
}

func (j *JWT) CreateToken(claims CustomClaims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(j.SigningKey)
}

func (j *JWT) ParserToken(tokenString string) (*CustomClaims, string) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return j.SigningKey, nil
	})

	if _, ok := err.(*jwt.ValidationError); ok {
		return nil, model.E_JWT_TOKEN_INVALID
	}

	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, model.E_SUCCESS
	} else {
		return nil, model.E_JWT_TOKEN_INVALID
	}
}
