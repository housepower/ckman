package common

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/housepower/ckman/log"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"
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
	f, err := os.CreateTemp(dir, prefix)
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

func ArraySearch(target string, str_array []string) bool {
	for _, str := range str_array {
		if target == str {
			return true
		}
	}
	return false
}

func ReplaceTemplateString(src *string, replace map[string]interface{}) error {
	t, err := template.New("T1").Parse(*src)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, replace)
	if err != nil {
		return err
	}
	*src = buf.String()
	return nil
}

func GetStringwithDefault(value, defaul string) string {
	if value == "" {
		return defaul
	}
	return value
}

func GetIntegerwithDefault(value, defaul int) int {
	if value == 0 {
		return defaul
	}
	return value
}

func Md5CheckSum(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:16])
}

// GetOutboundIP get preferred outbound ip of this machine
//https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Logger.Fatalf("need to setup the default route: %v", err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}


func ConvertDuration(start, end time.Time) string {
	hour, min, sec := 0,0,0
	d := int(end.Sub(start) / 1e9)
	sec = d % 60
	min = d / 60
	if min > 0 {
		hour = min / 60
		min = min % 60
	}
	var result string
	if hour > 0 {
		result = fmt.Sprintf("%dh ", hour)
	}
	if min > 0 || hour > 0{
		result += fmt.Sprintf("%dm ", min)
	}
	result += fmt.Sprintf("%ds", sec)
	return result
}