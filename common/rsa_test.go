package common

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	//fake key for test
	AUTHENTICATION_PUB_KEY = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDNw9cbOh1JRVNf/pQiRRMoa4TSmgZeq9zyK+Z5qE0Ak1XcmFzRg1m667ZAgfl/gEiwMGtbKyiPBGeHP5Gw3z5ENIHg7WGKTE0yRM/U/FMnktjly2xzjf7HUl/IA7PFYq5KBVBNPhjwzuFxpmsJL+fhhuYB75uDL0axYwcm7WHdewIDAQAB"
	AUTHENTICATION_PRI_KEY = "MIICXgIBAAKBgQDNw9cbOh1JRVNf/pQiRRMoa4TSmgZeq9zyK+Z5qE0Ak1XcmFzRg1m667ZAgfl/gEiwMGtbKyiPBGeHP5Gw3z5ENIHg7WGKTE0yRM/U/FMnktjly2xzjf7HUl/IA7PFYq5KBVBNPhjwzuFxpmsJL+fhhuYB75uDL0axYwcm7WHdewIDAQABAoGBAIKxMz1t6hAR4mUEc95YdVSlBhYmEomrK4j97UO0bERDULPuanYAscuRz46lf21Gc+TEvEuJ3BcKux8id00aXpcbbNhqIDyUMvET4MjdisgXhxay/dzc6jRBYQdhMrLT0NYfQSbULdXA3CGQhti4nChazn708ag6slvjGtsC4O9BAkEA9c/ZmbKisBb3GweWP/IhYB+GO5Qsby0KkF582NgnGIjnpGirniO2jyNSXO72QerTfG4JXqofGkH7AmlO0bkX0QJBANZLFBzoRIJr8x32dsKnd/V/7k2OgNbrUGwFJrJOGCSClPF7yM3xjN0lg3EjKW4AZP75pr//vOLOYTQDHyeNv4sCQQCoUlzyJ2XJ6N/q7WYQgbAjD1MuxwcqVhBuzZT2NAWJgm4EofwqvM/M8mX651NPzgploT/fR+UmaNoGS7BCYlmRAkAFAY3/uuFW1qTAT3CozXa88ncjsq+J1cd0Lo6f3bksqSxHk+e1/+2VgPnYG8Us/69cUYK2u4ezGLUmnOgOaX5PAkEA6wwIjYGDQRYIEVD4oJyNtdL7FFso63lon3LMySxLgi/KZGS4N8+FYJQVIzWWCrdk3Z1mXw4wuOQkE4pDy8xx+w=="
)

func TestAbc(t *testing.T) {
	var rsa RSAEncryption
	encode, _ := rsa.Encode([]byte("abc"), AUTHENTICATION_PRI_KEY)
	decode, _ := rsa.Decode(encode, AUTHENTICATION_PUB_KEY)
	//Oc/K4/uEvhVy4yZ1Ao2F2Acd7qxWiRHf4yzn9p+hiwwl1HRrC9a0hLMNk5zAkABnsAyQ2WJUdlidc50rN4S9qZe/mZ0cgJYsCyeXBJqumaiO6lXJQ64RGr3Fcs6YfDeLldfgDM4ul7/RM3uawLL2g239AGEClQrQfmFeJ9tWwnw=
	fmt.Println("encode:", string(encode))
	fmt.Println("decode:", string(decode))
	assert.Equal(t, "abc", string(decode))
}

func TestToken(t *testing.T) {
	var rsa RSAEncryption
	var userToken2 UserTokenModel
	userToken := UserTokenModel{
		Duration:           3600,
		RandomPaddingValue: "abc",
		UserId:             123456789,
		Timestamp:          time.Now().UnixNano() / 1e6,
	}

	fmt.Println("before: ", userToken)
	uenc, _ := json.Marshal(userToken)
	encode, _ := rsa.Encode(uenc, AUTHENTICATION_PRI_KEY)
	//dlAEsRx6cQvL6aEyYJSlj6CKHzXFktPe7dBSliPp+iYe8GVqdGijAqT3l45ofigKzKt8RI2U4upVnaLfwMAZ2s08ITb5yNENLdXHP0exiBvQwzdcul/3TaWMkPJ3rfIe9T5LYQDa5hJEoHnDnKkbjYg1dFk9H3p2mTpberPgTrk=
	decode, _ := rsa.Decode(encode, AUTHENTICATION_PUB_KEY)
	fmt.Println("encode:", string(encode))
	fmt.Println("decode:", string(decode))
	_ = json.Unmarshal(decode, &userToken2)
	fmt.Println("after: ", userToken2)

	assert.Equal(t, userToken, userToken2)

}
