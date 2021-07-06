package common

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

func GetWorkDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return ""
	}

	return strings.Replace(filepath.Dir(dir), "\\", "/", -1)
}

func VerifyPassword(pwd string) error {
	var hasNumber, hasUpperCase, hasLowercase, hasSpecial bool

	if len(pwd) < 8 {
		return errors.Errorf("password is only %d characters long", len(pwd))
	}

	for _, c := range pwd {
		switch {
		case unicode.IsNumber(c):
			hasNumber = true
		case unicode.IsUpper(c):
			hasUpperCase = true
		case unicode.IsLower(c):
			hasLowercase = true
		case unicode.IsPunct(c) || unicode.IsSymbol(c):
			hasSpecial = true
		}
	}

	typeNum := 0
	if hasNumber {
		typeNum++
	}
	if hasLowercase {
		typeNum++
	}
	if hasUpperCase {
		typeNum++
	}
	if hasSpecial {
		typeNum++
	}

	if typeNum < 3 {
		return errors.Errorf("password don't contain at least three character categories")
	}

	return nil
}

func HashPassword(pwd string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func ComparePassword(hashedPwd string, plainPwd string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPwd), []byte(plainPwd))
	return err == nil
}

func EnvStringVar(value *string, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		*value = val
	}
}

func EnvIntVar(value *int, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		valInt, err := strconv.Atoi(val)
		if err == nil {
			*value = valInt
		}
	}
}

func EnvBoolVar(value *bool, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	_, found := os.LookupEnv(realKey)
	if found {
		*value = true
	}
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

const (
	_         = iota
	KB uint64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

func ConvertDisk(size uint64) string {
	if size < KB {
		return fmt.Sprintf("%.2fB", float64(size)/float64(1))
	} else if size < MB {
		return fmt.Sprintf("%.2fKB", float64(size)/float64(KB))
	} else if size < GB {
		return fmt.Sprintf("%.2fMB", float64(size)/float64(MB))
	} else if size < TB {
		return fmt.Sprintf("%.2fGB", float64(size)/float64(GB))
	} else if size < PB {
		return fmt.Sprintf("%.2fTB", float64(size)/float64(TB))
	} else {
		return fmt.Sprintf("%.2fPB", float64(size)/float64(PB))
	}
}

func Decimal(value float64) float64 {
	value, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", value), 64)
	return value
}

type TempFile struct {
	BaseName string
	FullName string
}

func NewTempFile(dir, prefix string) (TempFile, error) {
	f, err := ioutil.TempFile(dir, prefix)
	if err != nil {
		return TempFile{}, err
	}
	defer f.Close()
	file := TempFile{
		BaseName: path.Base(f.Name()),
		FullName: f.Name(),
	}
	return file, nil
}

func DeepCopyByGob(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}
