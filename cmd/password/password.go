package password

import (
	"crypto/md5"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/housepower/ckman/common"
	"golang.org/x/term"
)

func PasswordHandle() {
	common.LoadUsers(path.Join(common.GetWorkDirectory(), "conf"))
	fmt.Println(`Password must be at least 8 characters long.
Password must contain at least three character categories among the following:
* Uppercase characters (A-Z)
* Lowercase characters (a-z)
* Digits (0-9)
* Special characters (~!@#$%^&*_-+=|\(){}[]:;"'<>,.?/)`)

	fmt.Printf("\nEnter username:")
	var username string
	fmt.Scanf("%s", &username)
	userinfo, err := common.GetUserInfo(username)
	if err != nil {
		fmt.Printf("\nGet user info fail: %v\n", err)
		return
	}
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
