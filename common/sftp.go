package common

import (
	"fmt"
	"github.com/pkg/sftp"
	log "github.com/sirupsen/logrus"
	"go-mysql-elasticsearch/config"
	"golang.org/x/crypto/ssh"
	"net"
	"os"
	"path"
	"time"
)

func SftpConnect(user, password, host string, port int) (*sftp.Client, error) {
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
		HostKeyCallback: func(host string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	// connet to ssh
	addr = fmt.Sprintf("%s:%d", host, port)

	if sshClient, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		return nil, err
	}

	// create sftp client
	if sftpClient, err = sftp.NewClient(sshClient); err != nil {
		return nil, err
	}

	return sftpClient, nil
}

func SyncFile(localFilePath, remoteDir string) {
	sftpClient, err := SftpConnect(config.Config.SftpConfig.User, config.Config.SftpConfig.Password, config.Config.SftpConfig.Host, config.Config.SftpConfig.Port)
	if err != nil {
		log.Fatal(err)
	}
	defer sftpClient.Close()

	srcFile, err := os.Open(localFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer srcFile.Close()

	var remoteFileName = path.Base(localFilePath)
	dstFile, err := sftpClient.Create(path.Join(remoteDir, remoteFileName))
	if err != nil {
		log.Fatal(err)
	}
	defer dstFile.Close()

	buf := make([]byte, 1024)
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		dstFile.Write(buf)
	}
	log.Info(fmt.Sprintf("copy %s to remote server finished!", localFilePath))
}
