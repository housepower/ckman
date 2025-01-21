package password

import (
	"crypto/md5"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/housepower/ckman/common"
	"golang.org/x/term"
)

func PasswordHandle(cwd string) {
	common.LoadUsers(path.Join(cwd, "conf"))
	//admin login
	fmt.Println("Welcome to ckman user management.")
	fmt.Printf("Please enter the password for admin user[ckman]:")
	adminPassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Printf("Read password fail: %v\n", err)
		return
	}
	err = login(adminPassword)
	if err != nil {
		fmt.Printf("\nLogin fail: %v\n", err)
		return
	}
	// list all users
	users := listUsers()
	fmt.Printf("\nPlease enter the user index:")
	var index int
	if _, err = fmt.Scanf("%d", &index); err != nil {
		fmt.Printf("\nEnter index fail: %v\n", err)
		return
	}
	if index < 0 || index >= len(users) {
		fmt.Printf("\nInvalid index: %d\n", index)
		return
	}

	if index == 0 {
		fmt.Printf("\nEnter username:")
		var username string
		fmt.Scanf("%s", &username)

		fmt.Printf("\nEnter policy(ordinary|guest):")
		var policy string
		fmt.Scanf("%s", &policy)
		if policy != common.ORDINARY && policy != common.GUEST {
			fmt.Printf("\nInvalid policy: %s\n", policy)
			return
		}
		users[index].Policy = policy
		users[index].UserFile = path.Join(cwd, "conf", "users", fmt.Sprintf("%s.%s", username, policy))
	}
	userinfo := users[index]
	username := path.Base(strings.Split(userinfo.UserFile, ".")[0])
	fmt.Println(`Password must be at least 8 characters long.
Password must contain at least three character categories among the following:
* Uppercase characters (A-Z)
* Lowercase characters (a-z)
* Digits (0-9)
* Special characters (~!@#$%^&*_-+=|\(){}[]:;"'<>,.?/)`)
	fmt.Printf("\nEnter password for [%s]: ", username)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Printf("\nEnter password fail: %v\n", err)
		return
	}

	password := string(bytePassword)
	if err := common.VerifyPassword(password); err != nil {
		fmt.Printf("\nVerify password fail: %v\n", err)
		return
	}

	fmt.Printf("\nReenter password for [%s]: ", username)
	dupPassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Printf("\nReenter password fail: %v\n", err)
		return
	}

	if password != string(dupPassword) {
		fmt.Println("\nPassword mismatch")
		return
	}

	md5str := fmt.Sprintf("%x", md5.Sum(bytePassword))
	hash, err := common.HashPassword(md5str)
	if err != nil {
		fmt.Printf("\nHash password fail: %v\n", err)
		return
	}

	fileFd, err := os.OpenFile(userinfo.UserFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Printf("\nOpen password file %s fail: %v\n", userinfo.UserFile, err)
		return
	}
	defer fileFd.Close()
	if _, err := fileFd.Write([]byte(hash)); err != nil {
		fmt.Printf("\nWrite password file %s fail: %v\n", userinfo.UserFile, err)
		return
	}

	fmt.Printf("\nSet password for [%s] success\n", username)
}

func login(bytePassword []byte) error {
	passwd := fmt.Sprintf("%x", md5.Sum(bytePassword))
	userinfo, err := common.GetUserInfo(common.DefaultAdminName)
	if err != nil {
		return err
	}
	if pass := common.ComparePassword(userinfo.Password, passwd); !pass {
		return fmt.Errorf("invalid password for ckman")
	}
	return nil
}

func listUsers() []common.UserInfo {
	i := 0
	var users []common.UserInfo
	fmt.Printf("\n%d. Create a New User\n", i)
	users = append(users, common.UserInfo{
		Password: "",
		Policy:   "",
		UserFile: "",
	})
	i++
	for name, info := range common.UserMap {
		fmt.Printf("%d. %s(%s)\n", i, name, info.Policy)
		users = append(users, info)
		i++
	}
	return users
}
