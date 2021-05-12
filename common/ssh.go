package common

import (
	"fmt"
	"github.com/housepower/ckman/log"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func SSHConnect(user, password, host string, port int) (*ssh.Client, error) {
	var (
		auth         []ssh.AuthMethod
		addr         string
		clientConfig *ssh.ClientConfig
		client       *ssh.Client
		err          error
	)
	// get auth method
	auth = make([]ssh.AuthMethod, 0)
	auth = append(auth, ssh.Password(password))

	clientConfig = &ssh.ClientConfig{
		User:            user,
		Auth:            auth,
		Timeout:         30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// connet to ssh
	addr = fmt.Sprintf("%s:%d", host, port)

	if client, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	return client, nil
}

func SFTPConnect(user, password, host string, port int) (*sftp.Client, error) {
	var (
		auth         []ssh.AuthMethod
		addr         string
		clientConfig *ssh.ClientConfig
		sshClient    *ssh.Client
		sftpClient   *sftp.Client
		err          error
	)
	// get auth method
	auth = make([]ssh.AuthMethod, 0)
	auth = append(auth, ssh.Password(password))

	clientConfig = &ssh.ClientConfig{
		User:    user,
		Auth:    auth,
		Timeout: 30 * time.Second,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	// connet to ssh
	addr = fmt.Sprintf("%s:%d", host, port)

	if sshClient, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	// create sftp client
	if sftpClient, err = sftp.NewClient(sshClient); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	return sftpClient, nil
}

func SFTPUpload(sftpClient *sftp.Client, localFilePath, remoteDir string) error {
	srcFile, err := os.Open(localFilePath)
	if err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer srcFile.Close()

	var remoteFileName = path.Base(localFilePath)
	dstFile, err := sftpClient.Create(path.Join(remoteDir, remoteFileName))
	if err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer dstFile.Close()

	buf := make([]byte, 1024*1024)
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		dstFile.Write(buf[0:n])
	}

	return nil
}

func SFTPDownload(sftpClient *sftp.Client, remoteFilePath, localDir string) error {
	var remoteFileName = path.Base(remoteFilePath)
	dstFile, err := os.Create(path.Join(localDir, remoteFileName))
	if err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer dstFile.Close()

	srcFile, err := sftpClient.Open(remoteFilePath)
	if err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer srcFile.Close()

	buf := make([]byte, 1024*1024)
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		dstFile.Write(buf[0:n])
	}

	return nil
}

func SSHRun(client *ssh.Client, shell string) (result string, err error) {
	var session *ssh.Session
	var buf []byte
	// create session
	if session, err = client.NewSession(); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer session.Close()
	if buf, err = session.CombinedOutput(shell); err != nil {
		result = strings.TrimRight(string(buf), "\n")
		err = errors.Wrapf(err, result)
		return
	}
	result = strings.TrimRight(string(buf), "\n")
	return
}

func ScpFiles(files []string, path, user, password, ip string, port int) error {
	sftpClient, err := SFTPConnect(user, password, ip, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	for _, file := range files {
		err = SFTPUpload(sftpClient, file, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func ScpDownloadFiles(files []string, path, user, password, ip string, port int) error {
	sftpClient, err := SFTPConnect(user, password, ip, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	for _, file := range files {
		err = SFTPDownload(sftpClient, file, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func RemoteExecute(user, password, host string, port int, cmd string)(string, error) {
	client, err := SSHConnect(user, password, host, port)
	if err != nil {
		return "", err
	}
	defer client.Close()
	var output string
	if output, err = SSHRun(client, cmd); err != nil {
		log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
		return "", err
	}
	return output, nil
}