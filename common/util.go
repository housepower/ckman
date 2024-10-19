package common

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/housepower/ckman/log"

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
		return "", errors.Wrap(err, "")
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

type TempFile struct {
	BaseName string
	FullName string
}

func NewTempFile(dir, prefix string) (TempFile, error) {
	f, err := os.CreateTemp(dir, prefix)
	if err != nil {
		return TempFile{}, errors.Wrap(err, "")
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
		return errors.Wrap(err, "")
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func ReplaceTemplateString(src *string, replace map[string]interface{}) error {
	t, err := template.New("T1").Parse(*src)
	if err != nil {
		return errors.Wrap(err, "")
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, replace)
	if err != nil {
		return errors.Wrap(err, "")
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

// GetOutboundIP get preferred outbound ip of this machine
// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() net.IP {
	if hostIp := os.Getenv("HOST_IP"); hostIp != "" {
		return net.ParseIP(hostIp)
	}

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Logger.Fatalf("need to setup the default route: %v", err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

func ConvertDuration(start, end time.Time) string {
	hour, min, sec := 0, 0, 0
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
	if min > 0 || hour > 0 {
		result += fmt.Sprintf("%dm ", min)
	}
	result += fmt.Sprintf("%ds", sec)
	return result
}

func FormatReadableTime(seconds uint32) string {
	var result string
	var day = seconds / (24 * 3600)
	if day > 0 {
		result += fmt.Sprintf("%dd", day)
	}
	hour := (seconds - day*3600*24) / 3600
	if hour > 0 {
		result += fmt.Sprintf("%dh", hour)
	}
	minute := (seconds - day*24*3600 - hour*3600) / 60
	if minute > 0 {
		result += fmt.Sprintf("%dm", minute)
	}
	second := seconds - day*24*3600 - hour*3600 - minute*60
	result += fmt.Sprintf("%ds", second)
	return result
}

func Shuffle(value []string) []string {
	rand.Seed(time.Now().UnixNano())

	arr := make([]string, len(value))
	copy(arr, value)

	rand.Shuffle(len(arr), func(i, j int) {
		arr[i], arr[j] = arr[j], arr[i]
	})

	return arr
}

func ArrayDistinct[T string | int | int64 | int32 | uint | uint64 | uint32 | float32 | float64](arr []T) []T {
	set := make(map[T]struct{}, len(arr))
	j := 0
	for _, v := range arr {
		_, ok := set[v]
		if ok {
			continue
		}
		set[v] = struct{}{}
		arr[j] = v
		j++
	}
	return arr[:j]
}

func ArrayRemove[T string | int | int64 | int32 | uint | uint64 | uint32 | float32 | float64](arr []T, elem T) []T {
	var res []T
	for _, v := range arr {
		if v == elem {
			continue
		}
		res = append(res, v)
	}
	return res
}

func ArraySearch[T string | int | int64 | int32 | uint | uint64 | uint32 | float32 | float64](target T, str_array []T) bool {
	for _, str := range str_array {
		if target == str {
			return true
		}
	}
	return false
}

func TernaryExpression(condition bool, texpr, fexpr interface{}) interface{} {
	if condition {
		return texpr
	} else {
		return fexpr
	}
}

func EnsurePathNonPrefix(paths []string) error {
	if len(paths) < 2 {
		return nil
	}
	sort.Strings(paths)
	for i := 1; i < len(paths); i++ {
		curr := paths[i]
		prev := paths[i-1]
		if prev == curr {
			return errors.Errorf("path %s is duplicate", curr)
		}
		if strings.HasPrefix(curr, prev) {
			return errors.Errorf("path %s is subdir of path %s", curr, prev)
		}
	}
	return nil
}
