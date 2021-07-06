package common

import (
	"bytes"
	"crypto/des"
	"encoding/hex"
)

const DES_KEY  = "eoickman"


func DesEncrypt(text string) string{
	src := []byte(text)
	block, err := des.NewCipher([]byte(DES_KEY))
	if err != nil {
		return text
	}
	bs := block.BlockSize()
	src = ZeroPadding(src, bs)
	if len(src)%bs != 0 {
		return text
	}
	out := make([]byte, len(src))
	dst := out
	for len(src) > 0 {
		block.Encrypt(dst, src[:bs])
		src = src[bs:]
		dst = dst[bs:]
	}
	return hex.EncodeToString(out)
}

func DesDecrypt(decrypted string)string {
	src, err := hex.DecodeString(decrypted)
	if err != nil {
		return decrypted
	}
	block, err := des.NewCipher([]byte(DES_KEY))
	if err != nil {
		return decrypted
	}
	out := make([]byte, len(src))
	dst := out
	bs := block.BlockSize()
	if len(src)%bs != 0 {
		return decrypted
	}
	for len(src) > 0 {
		block.Decrypt(dst, src[:bs])
		src = src[bs:]
		dst = dst[bs:]
	}
	out = ZeroUnPadding(out)
	return string(out)
}

func ZeroPadding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{0}, padding)
	return append(ciphertext, padtext...)
}

func ZeroUnPadding(origData []byte) []byte {
	return bytes.TrimFunc(origData,
		func(r rune) bool {
			return r == rune(0)
		})
}
