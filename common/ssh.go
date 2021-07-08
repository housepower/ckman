package common

import (
	"bytes"
	"fmt"
	"github.com/housepower/ckman/log"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func sshConnectwithPassword(user, password string) (*ssh.ClientConfig, error) {
	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		Timeout: 30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}

func sshConnectwithPublickKey(user string) (*ssh.ClientConfig, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadFile(path.Join(homePath, ".ssh", "id_rsa"))
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		Timeout:         30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}

func SSHConnect(user, password, host string, port int) (*ssh.Client, error) {
	var (
		addr         string
		clientConfig *ssh.ClientConfig
		client       *ssh.Client
		err          error
	)

	if password == "" {
		clientConfig,err  = sshConnectwithPublickKey(user)
	} else {
		clientConfig,err  = sshConnectwithPassword(user, password)
	}
	if err != nil {
		return nil, err
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
		addr         string
		clientConfig *ssh.ClientConfig
		sshClient    *ssh.Client
		sftpClient   *sftp.Client
		err          error
	)

	if password == "" {
		clientConfig,err  = sshConnectwithPublickKey(user)
	} else {
		clientConfig,err  = sshConnectwithPassword(user, password)
	}
	if err != nil {
		return nil, err
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

func SFTPUpload(sftpClient *sftp.Client, localFilePath, remoteFilePath string) error {
	srcFile, err := os.Open(localFilePath)
	if err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer srcFile.Close()

	dstFile, err := sftpClient.Create(remoteFilePath)
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
		_, _ = dstFile.Write(buf[0:n])
	}

	return nil
}

func SFTPDownload(sftpClient *sftp.Client, remoteFilePath, localFilePath string) error {
	dstFile, err := os.Create(localFilePath)
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
		_, _ = dstFile.Write(buf[0:n])
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
	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if err = session.Run(shell); err != nil {
		errMsg := stderr.Bytes()
		err = errors.Wrapf(err, strings.TrimRight(string(errMsg), "\n"))
		return
	}
	buf = stdout.Bytes()
	result = strings.TrimRight(string(buf), "\n")
	return
}

func ScpUploadFiles(files []string, remotePath, user, password, ip string, port int) error {
	sftpClient, err := SFTPConnect(user, password, ip, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	for _, file := range files {
		if file == "" {
			continue
		}
		baseName := path.Base(file)
		err = SFTPUpload(sftpClient, file, path.Join(remotePath, baseName))
		if err != nil {
			return err
		}
	}
	return nil
}

func ScpUploadFile(localFile, remoteFile, user, password, ip string, port int) error {
	sftpClient, err := SFTPConnect(user, password, ip, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	err = SFTPUpload(sftpClient, localFile, remoteFile)
	if err != nil {
		return err
	}
	return nil
}

func ScpDownloadFiles(files []string, localPath, user, password, ip string, port int) error {
	sftpClient, err := SFTPConnect(user, password, ip, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	for _, file := range files {
		baseName := path.Base(file)
		err = SFTPDownload(sftpClient, file, path.Join(localPath, baseName))
		if err != nil {
			return err
		}
	}
	return nil
}

func ScpDownloadFile(remoteFile, localFile, user, password, ip string, port int) error {
	sftpClient, err := SFTPConnect(user, password, ip, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	err = SFTPDownload(sftpClient, remoteFile, localFile)
	if err != nil {
		return err
	}
	return nil
}

func RemoteExecute(user, password, host string, port int, cmd string) (string, error) {
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
