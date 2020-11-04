package common

import (
	"github.com/dgrijalva/jwt-go"
	"gitlab.eoitek.net/EOI/ckman/model"
)

var (
	DefaultUserName   = "ckman"
	DefaultSigningKey = "change me"
	TokenExpireTime   = 3600
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

func (j *JWT) ParserToken(tokenString string) (*CustomClaims, int) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return j.SigningKey, nil
	})

	if err != nil {
		if ve, ok := err.(*jwt.ValidationError); ok {
			if ve.Errors&jwt.ValidationErrorExpired != 0 {
				return nil, model.JWT_TOKEN_EXPIRED
			} else {
				return nil, model.JWT_TOKEN_INVALID
			}

		}
	}

	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, model.SUCCESS
	} else {
		return nil, model.JWT_TOKEN_INVALID
	}
}
