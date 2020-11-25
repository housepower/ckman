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
// @Failure 200 {string} json "{"code":5004,"msg":"上传安装包失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
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
// @Failure 200 {string} json "{"code":5005,"msg":"获取安装包列表失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":["20.8.5.45"]}"
// @Router /api/v1/package [get]
func (p *PackageController) List(c *gin.Context) {
	files, err := GetAllFiles(path.Join(common.GetWorkDirectory(), DefaultPackageDirectory))
	if err != nil {
		model.WrapMsg(c, model.LIST_PACKAGE_FAIL, model.GetMsg(model.LIST_PACKAGE_FAIL), err.Error())
		return
	}

	versions := GetAllVersions(files)
	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), versions)
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

func GetAllVersions(files []string) []string {
	versions := make([]string, 0)
	ckClientMap := make(map[string]bool)
	ckCommonMap := make(map[string]bool)
	ckServerMap := make(map[string]bool)

	for _, file := range files {
		end := strings.LastIndex(file, "-")
		if strings.HasPrefix(file, model.CkClientPackagePrefix) && strings.HasSuffix(file, model.CkClientPackageSuffix) {
			start := len(model.CkClientPackagePrefix) + 1
			version := file[start:end]
			ckClientMap[version] = true
			continue
		}
		if strings.HasPrefix(file, model.CkCommonPackagePrefix) && strings.HasSuffix(file, model.CkCommonPackageSuffix) {
			start := len(model.CkCommonPackagePrefix) + 1
			version := file[start:end]
			ckCommonMap[version] = true
			continue
		}
		if strings.HasPrefix(file, model.CkServerPackagePrefix) && strings.HasSuffix(file, model.CkServerPackageSuffix) {
			start := len(model.CkServerPackagePrefix) + 1
			version := file[start:end]
			ckServerMap[version] = true
			continue
		}
	}

	for key, _ := range ckCommonMap {
		_, clientOk := ckClientMap[key]
		_, serverOk := ckServerMap[key]
		if clientOk && serverOk {
			versions = append(versions, key)
		}
	}

	return versions
}

// @Summary 删除包
// @Description 删除包
// @version 1.0
// @Security ApiKeyAuth
// @Param packageVersion query string true "package version" default(20.8.5.45)
// @Failure 200 {string} json "{"code":5002,"msg":"删除ClickHouse表失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/package [delete]
func (p *PackageController) Delete(c *gin.Context) {
	packageVersion := c.Query("packageVersion")
	packages := make([]string, 3)

	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, packageVersion, model.CkClientPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, packageVersion, model.CkCommonPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, packageVersion, model.CkServerPackageSuffix)

	for _, packageName := range packages {
		if err := os.Remove(path.Join(common.GetWorkDirectory(), DefaultPackageDirectory, packageName)); err != nil {
			model.WrapMsg(c, model.DELETE_LOCAL_PACKAGE_FAIL, model.GetMsg(model.DELETE_LOCAL_PACKAGE_FAIL), err.Error())
			return
		}
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
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package?packageVersion=%s", peer, p.config.Server.Port, packageVersion)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					model.WrapMsg(c, model.DELETE_PEER_PACKAGE_FAIL, model.GetMsg(model.DELETE_PEER_PACKAGE_FAIL), err.Error())
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package?packageVersion=%s", peer, p.config.Server.Port, packageVersion)
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
