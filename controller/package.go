package controller

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/common"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	DefaultPackageDirectory string = "package"
	FormPackageFieldName    string = "package"
)

type PackageController struct {
	config *config.CKManConfig
}

func NewPackageController(config *config.CKManConfig) *PackageController {
	ck := &PackageController{}
	ck.config = config
	return ck
}

// @Summary 上传安装包
// @Description 上传安装包
// @version 1.0
// @Security ApiKeyAuth
// @accept multipart/form-data
// @Param package formData file true "安装包"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":5004,"msg":"上传安装包失败","data":""}"
// @Router /api/v1/package [post]
func (p *PackageController) Upload(c *gin.Context) {
	localFile, err := ParserFormData(c.Request)
	if err != nil {
		model.WrapMsg(c, model.UPLOAD_LOCAL_PACKAGE_FAIL, model.GetMsg(model.UPLOAD_LOCAL_PACKAGE_FAIL), err.Error())
		return
	}

	reqFromPeer := false
	requestIP := c.ClientIP()
	for _, peer := range p.config.Server.Peers {
		if strings.Compare(requestIP, peer) == 0 {
			reqFromPeer = true
			break
		}
	}

	// 防止相互传，进入死循环
	if !reqFromPeer {
		for _, peer := range p.config.Server.Peers {
			peerUrl := ""
			if p.config.Server.Https {
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package", peer, p.config.Server.Port)
				err = UploadFileByURL(peerUrl, localFile)
				if err != nil {
					model.WrapMsg(c, model.UPLOAD_PEER_PACKAGE_FAIL, model.GetMsg(model.UPLOAD_PEER_PACKAGE_FAIL), err.Error())
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package", peer, p.config.Server.Port)
				err = UploadFileByURL(peerUrl, localFile)
				if err != nil {
					model.WrapMsg(c, model.UPLOAD_PEER_PACKAGE_FAIL, model.GetMsg(model.UPLOAD_PEER_PACKAGE_FAIL), err.Error())
					return
				}
			}
		}
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

func ParserFormData(request *http.Request) (string, error) {
	// Parse multipart form, 200 << 20 specifies a maximum upload of 200 MB files
	request.ParseMultipartForm(200 << 20)
	// FormFile returns the first file for the given key `file`
	// It also returns the FileHeader so we can get the Filename, Header and the size of the file
	clientFd, handler, err := request.FormFile(FormPackageFieldName)
	if err != nil {
		log.Logger.Errorf("Form file fail: %v", err)
		return "", err
	}

	log.Logger.Infof("Upload File: %s", handler.Filename)
	log.Logger.Infof("File Size: %d", handler.Size)
	localFile := path.Join(common.GetWorkDirectory(), DefaultPackageDirectory, handler.Filename)
	localFd, err := os.OpenFile(localFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Logger.Errorf("Create local file %s fail: %v", localFile, err)
		return "", err
	}
	defer localFd.Close()

	_, err = io.Copy(localFd, clientFd)
	if err != nil {
		log.Logger.Errorf("Write local file %s fail: %v", localFile, err)
		os.Remove(localFile)
		return "", err
	}

	return localFile, nil
}

func UploadFileByURL(url string, localFile string) error {
	file, err := os.Open(localFile)
	if err != nil {
		return err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(FormPackageFieldName, filepath.Base(localFile))
	if err != nil {
		return err
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return err
	}
	writer.Close()

	request, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return fmt.Errorf("%s", response.Status)
	}

	return nil
}

// @Summary 获取安装包列表
// @Description 获取安装包列表
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":5005,"msg":"获取安装包列表失败","data":""}"
// @Router /api/v1/package [get]
func (p *PackageController) List(c *gin.Context) {
	files, err := GetAllFiles(path.Join(common.GetWorkDirectory(), DefaultPackageDirectory))
	if err != nil {
		model.WrapMsg(c, model.LIST_PACKAGE_FAIL, model.GetMsg(model.LIST_PACKAGE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), files)
}

func GetAllFiles(dirPth string) ([]string, error) {
	files := make([]string, 0)

	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}

	for _, fi := range dir {
		if !fi.IsDir() {
			if ok := strings.HasSuffix(fi.Name(), ".rpm"); ok {
				files = append(files, fi.Name())
			}
		}
	}

	sort.Strings(files)
	return files, nil
}

// @Summary 删除包
// @Description 删除包
// @version 1.0
// @Security ApiKeyAuth
// @Param packageName query string true "package name"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":5002,"msg":"删除ClickHouse表失败","data":""}"
// @Router /api/v1/package [delete]
func (p *PackageController) Delete(c *gin.Context) {
	packageName := c.Query("packageName")

	if err := os.Remove(path.Join(common.GetWorkDirectory(), DefaultPackageDirectory, packageName)); err != nil {
		model.WrapMsg(c, model.DELETE_LOCAL_PACKAGE_FAIL, model.GetMsg(model.DELETE_LOCAL_PACKAGE_FAIL), err.Error())
		return
	}

	reqFromPeer := false
	requestIP := c.ClientIP()
	for _, peer := range p.config.Server.Peers {
		if strings.Compare(requestIP, peer) == 0 {
			reqFromPeer = true
			break
		}
	}

	// 防止循环删除
	if !reqFromPeer {
		for _, peer := range p.config.Server.Peers {
			peerUrl := ""
			if p.config.Server.Https {
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package?packageName=%s", peer, p.config.Server.Port, packageName)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					model.WrapMsg(c, model.DELETE_PEER_PACKAGE_FAIL, model.GetMsg(model.DELETE_PEER_PACKAGE_FAIL), err.Error())
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package?packageName=%s", peer, p.config.Server.Port, packageName)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					model.WrapMsg(c, model.DELETE_PEER_PACKAGE_FAIL, model.GetMsg(model.DELETE_PEER_PACKAGE_FAIL), err.Error())
					return
				}
			}
		}
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

func DeleteFileByURL(url string) error {
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return fmt.Errorf("%s", response.Status)
	}

	return nil
}