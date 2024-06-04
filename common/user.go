package common

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
)

const (
	ADMIN    string = "ckman"
	GUEST    string = "guest"
	ORDINARY string = "ordinary"

	DefaultAdminName     = "ckman"
	InternalOrdinaryName = "ordinary"
)

type UserInfo struct {
	Policy   string
	Password string
	UserFile string
}

var UserMap map[string]UserInfo
var lock sync.Mutex

func LoadUsers(configPath string) {
	//usermap
	lock.Lock()
	defer lock.Unlock()
	UserMap = make(map[string]UserInfo)
	userPath := path.Join(configPath, "users")
	entries, err := os.ReadDir(userPath)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			userfile := path.Join(userPath, entry.Name())
			userinfos := strings.Split(entry.Name(), ".")
			if len(userinfos) != 2 {
				continue
			}
			username := userinfos[0]
			policy := userinfos[1]
			if policy != GUEST && policy != ORDINARY {
				continue
			}
			password, err := os.ReadFile(userfile)
			if err == nil {
				UserMap[username] = UserInfo{
					Policy:   policy,
					Password: string(password),
					UserFile: userfile,
				}
			}
		}
	}

	passwordFile := path.Join(configPath, "password")
	password, err := os.ReadFile(passwordFile)
	if err != nil {
		return
	}

	UserMap[DefaultAdminName] = UserInfo{
		Policy:   ADMIN,
		Password: string(password),
		UserFile: passwordFile,
	}

	UserMap[InternalOrdinaryName] = UserInfo{
		Policy: ORDINARY,
		//Password: "change me",	// InternalOrdinaryName 专门给userToken方式的用户使用，无需密码
	}
}

func GetUserInfo(name string) (UserInfo, error) {
	lock.Lock()
	defer lock.Unlock()
	if userinfo, ok := UserMap[name]; ok {
		return userinfo, nil
	}
	return UserInfo{}, fmt.Errorf("user %s doesn't exist", name)
}
