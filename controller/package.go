package controller

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

const (
	FormPackageFieldName string = "package"
	InitialByClusterPeer string = "is_initial_by_cluster_peer"
)

type PackageController struct {
	Controller
	config *config.CKManConfig
}

func NewPackageController(config *config.CKManConfig, wrapfunc Wrapfunc) *PackageController {
	pc := &PackageController{}
	pc.config = config
	pc.wrapfunc = wrapfunc
	return pc
}

// @Summary 上传安装包
// @Description 需要同时上传三个包，包括client、server和common
// @version 1.0
// @Security ApiKeyAuth
// @Tags package
// @Accept  json
// @accept multipart/form-data
// @Param package formData file true "package"
// @Failure 200 {string} json "{"code":"5202","msg":"upload local package failed","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/package [post]
func (controller *PackageController) Upload(c *gin.Context) {
	localFile, err := ParserFormData(c.Request)
	if err != nil {
		controller.wrapfunc(c, model.E_UPLOAD_FAILED, err)
		return
	}

	reqFromPeer := false
	ret := c.GetHeader(InitialByClusterPeer)
	if ret == "true" {
		reqFromPeer = true
	}
	if !reqFromPeer {
		for _, peer := range config.GetClusterPeers() {
			peerUrl := ""
			if controller.config.Server.Https {
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package", peer.Ip, peer.Port)
				err = UploadFileByURL(peerUrl, localFile)
				if err != nil {
					controller.wrapfunc(c, model.E_UPLOAD_FAILED, err)
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package", peer.Ip, peer.Port)
				err = UploadFileByURL(peerUrl, localFile)
				if err != nil {
					controller.wrapfunc(c, model.E_UPLOAD_FAILED, err)
					return
				}
			}
		}
	}

	err = common.LoadPackages()
	if err != nil {
		controller.wrapfunc(c, model.E_UPLOAD_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

func ParserFormData(request *http.Request) (string, error) {
	// Parse multipart form, 200 << 20 specifies a maximum upload of 200 MB files
	_ = request.ParseMultipartForm(200 << 20)
	// FormFile returns the first file for the given key `file`
	// It also returns the FileHeader so we can get the Filename, Header and the size of the file
	clientFd, handler, err := request.FormFile(FormPackageFieldName)
	if err != nil {
		log.Logger.Errorf("Form file fail: %v", err)
		return "", errors.Wrap(err, "")
	}

	log.Logger.Infof("Upload File: %s", handler.Filename)
	log.Logger.Infof("File Size: %d", handler.Size)
	dir := path.Join(config.GetWorkDirectory(), common.DefaultPackageDirectory)
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0777)
		if err != nil {
			return "", errors.Wrap(err, "")
		}
	}
	localFile := path.Join(dir, handler.Filename)
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
		return "", errors.Wrap(err, "")
	}

	return localFile, nil
}

func UploadFileByURL(url string, localFile string) error {
	file, err := os.Open(localFile)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(FormPackageFieldName, filepath.Base(localFile))
	if err != nil {
		return errors.Wrap(err, "")
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return errors.Wrap(err, "")
	}
	writer.Close()

	request, err := http.NewRequest("POST", url, body)
	if err != nil {
		return errors.Wrap(err, "")
	}
	request.Header.Add("Content-Type", writer.FormDataContentType())
	request.Header.Add(InitialByClusterPeer, "true")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return errors.Errorf("%s", response.Status)
	}

	return nil
}

// @Summary 获取安装包列表
// @Description 获取安装包列表
// @version 1.0
// @Security ApiKeyAuth
// @Tags package
// @Accept  json
// @Param pkgType query string true "pkgType" default(all)
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"version":"22.3.9.19","pkgType":"x86_64.rpm","pkgName":"clickhouse-common-static-22.3.9.19.x86_64.rpm"}]}"
// @Router /api/v2/package [get]
func (controller *PackageController) List(c *gin.Context) {
	pkgType := c.Query("pkgType")
	if pkgType == "" {
		pkgType = model.PkgTypeDefault
	}

	pkgs := common.GetAllPackages()
	var resp []model.PkgInfo
	if pkgType == "all" {
		for k, v := range pkgs {
			for _, p := range v {
				pi := model.PkgInfo{
					Version: p.Version,
					PkgType: k,
					PkgName: p.PkgName,
				}
				resp = append(resp, pi)
			}
		}
	} else {
		v := pkgs[pkgType]
		for _, p := range v {
			pi := model.PkgInfo{
				Version: p.Version,
				PkgType: pkgType,
				PkgName: p.PkgName,
			}
			resp = append(resp, pi)
		}
	}
	controller.wrapfunc(c, model.E_SUCCESS, resp)
}

// @Summary 删除安装包
// @Description 删除安装包
// @version 1.0
// @Security ApiKeyAuth
// @Tags package
// @Accept  json
// @Param packageVersion query string true "package version" default(22.3.9.19)
// @Param pkgType query string true "package type" default(x86_64.rpm)
// @Failure 200 {string} json "{"code":"5201","msg":"文件不存在","data":""}"
// @Failure 200 {string} json "{"code":"5803","msg":"删除数据失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"查询数据失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/package [delete]
func (controller *PackageController) Delete(c *gin.Context) {
	packageVersion := c.Query("packageVersion")
	packageType := c.Query("pkgType")
	packages := deploy.BuildPackages(packageVersion, packageType, "")
	for _, packageName := range packages.PkgLists {
		if err := os.Remove(path.Join(config.GetWorkDirectory(), common.DefaultPackageDirectory, packageName)); err != nil {
			controller.wrapfunc(c, model.E_FILE_NOT_EXIST, err)
			return
		}
	}

	reqFromPeer := false
	ret := c.GetHeader(InitialByClusterPeer)
	if ret == "true" {
		reqFromPeer = true
	}

	if !reqFromPeer {
		for _, peer := range config.GetClusterPeers() {
			peerUrl := ""
			if controller.config.Server.Https {
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package?packageVersion=%s&packageType=%s", peer.Ip, peer.Port, packageVersion, packageType)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package?packageVersion=%s&packageType=%s", peer.Ip, peer.Port, packageVersion, packageType)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
					return
				}
			}
		}
	}

	err := common.LoadPackages()
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

func DeleteFileByURL(url string) error {
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return errors.Wrap(err, "")
	}
	request.Header.Add(InitialByClusterPeer, "true")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return errors.Errorf("%s", response.Status)
	}

	return nil
}
