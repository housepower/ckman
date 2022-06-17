package common

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

/*
The token is resolved with fixed public key and private key.
The unified portal uses the private key to encrypt the token,
and ckman only needs to decrypt it with the public key.
*/
const (
	AUTHENTICATION_PUB_KEY_PREFIX = "-----BEGIN RSA Public Key-----\n"
	AUTHENTICATION_PRI_KEY_PREFIX = "-----BEGIN RSA Private Key-----\n"
	AUTHENTICATION_PUB_KEY_SUFFIX = "\n-----END RSA Public Key-----"
	AUTHENTICATION_PRI_KEY_SUFFIX = "\n-----END RSA Private Key-----"
)

type UserTokenModel struct {
	Duration           int64
	RandomPaddingValue string
	UserId             int64
	Timestamp          int64
}

type RSAEncryption struct{}

func (encry RSAEncryption) GenPublicKey(publicKey string) []byte {
	return []byte(fmt.Sprintf("%s%s%s", AUTHENTICATION_PUB_KEY_PREFIX, publicKey, AUTHENTICATION_PUB_KEY_SUFFIX))
}

func (encry RSAEncryption) GenPrivateKey(privateKey string) []byte {
	return []byte(fmt.Sprintf("%s%s%s", AUTHENTICATION_PRI_KEY_PREFIX, privateKey, AUTHENTICATION_PRI_KEY_SUFFIX))
}

func (encry RSAEncryption) GetPublicKey(publicKey string) (*rsa.PublicKey, error) {
	// decode public key

	block, _ := pem.Decode(encry.GenPublicKey(publicKey))
	if block == nil {
		return nil, errors.New("get public key error")
	}
	// x509 parse public key
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return pub.(*rsa.PublicKey), err
}

func (encry RSAEncryption) GetPrivateKey(privateKey string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(encry.GenPrivateKey(privateKey))
	if block == nil {
		return nil, errors.New("get private key error")
	}
	pri, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return pri, nil
	}
	pri2, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return pri2.(*rsa.PrivateKey), nil
}

func (encry RSAEncryption) Decode(encode []byte, publicKey string) ([]byte, error) {
	databs, _ := base64.StdEncoding.DecodeString(string(encode))
	pubkey, _ := encry.GetPublicKey(publicKey)
	if pubkey == nil {
		return []byte(""), errors.New("Please set the public key in advance\n")
	}
	output := bytes.NewBuffer(nil)
	err := pubKeyIO(pubkey, bytes.NewReader(databs), output, false)
	if err != nil {
		return []byte(""), err
	}
	return io.ReadAll(output)
}

// ckman do not need encode token
func (encry RSAEncryption) Encode(decode []byte, privateKey string) ([]byte, error) {
	prikey, _ := encry.GetPrivateKey(privateKey)
	if prikey == nil {
		return []byte(""), errors.New("Please set the private key in advance\n")
	}
	output := bytes.NewBuffer(nil)
	err := priKeyIO(prikey, bytes.NewReader(decode), output, true)
	if err != nil {
		return []byte(""), err
	}
	rsadata, err := io.ReadAll(output)
	if err != nil {
		return []byte(""), errors.Wrap(err, "")
	}
	return []byte(base64.StdEncoding.EncodeToString(rsadata)), nil
}
