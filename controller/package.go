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
	config *config.CKManConfig
}

func NewPackageController(config *config.CKManConfig) *PackageController {
	ck := &PackageController{}
	ck.config = config
	return ck
}

// @Summary Upload package
// @Description Upload package
// @version 1.0
// @Security ApiKeyAuth
// @accept multipart/form-data
// @Param package formData file true "package"
// @Failure 200 {string} json "{"retCode":"5004","retMsg":"upload local package failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/package [post]
func (p *PackageController) Upload(c *gin.Context) {
	localFile, err := ParserFormData(c.Request)
	if err != nil {
		model.WrapMsg(c, model.UPLOAD_LOCAL_PACKAGE_FAIL, err)
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
			if p.config.Server.Https {
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package", peer.Ip, peer.Port)
				err = UploadFileByURL(peerUrl, localFile)
				if err != nil {
					model.WrapMsg(c, model.UPLOAD_PEER_PACKAGE_FAIL, err)
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package", peer.Ip, peer.Port)
				err = UploadFileByURL(peerUrl, localFile)
				if err != nil {
					model.WrapMsg(c, model.UPLOAD_PEER_PACKAGE_FAIL, err)
					return
				}
			}
		}
	}

	err = common.GetPackages()
	if err != nil {
		model.WrapMsg(c, model.UPLOAD_PEER_PACKAGE_FAIL, err)
		return
	}
	model.WrapMsg(c, model.SUCCESS, nil)
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

// @Summary Get package list
// @Description Get package list
// @version 1.0
// @Security ApiKeyAuth
// @Param pkgType query string true "pkgType" default(all)
// @Failure 200 {string} json "{"retCode":"5005","retMsg":"get package list failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"version":"22.3.9.19","pkgType":"x86_64.rpm","pkgName":"clickhouse-common-static-22.3.9.19.x86_64.rpm"}]}"
// @Router /api/v1/package [get]
func (p *PackageController) List(c *gin.Context) {
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
	model.WrapMsg(c, model.SUCCESS, resp)
}

// @Summary Delete package
// @Description Delete package
// @version 1.0
// @Security ApiKeyAuth
// @Param packageVersion query string true "package version" default(22.3.9.19)
// @Param pkgType query string true "package type" default(x86_64.rpm)
// @Failure 200 {string} json "{"retCode":"5006","retMsg":"delete local package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5007","retMsg":"delete peer package failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/package [delete]
func (p *PackageController) Delete(c *gin.Context) {
	packageVersion := c.Query("packageVersion")
	packageType := c.Query("pkgType")
	packages := deploy.BuildPackages(packageVersion, packageType, "")
	for _, packageName := range packages.PkgLists {
		if err := os.Remove(path.Join(config.GetWorkDirectory(), common.DefaultPackageDirectory, packageName)); err != nil {
			model.WrapMsg(c, model.DELETE_LOCAL_PACKAGE_FAIL, err)
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
			if p.config.Server.Https {
				peerUrl = fmt.Sprintf("https://%s:%d/api/v1/package?packageVersion=%s", peer.Ip, peer.Port, packageVersion)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					model.WrapMsg(c, model.DELETE_PEER_PACKAGE_FAIL, err)
					return
				}
			} else {
				peerUrl = fmt.Sprintf("http://%s:%d/api/v1/package?packageVersion=%s", peer.Ip, peer.Port, packageVersion)
				err := DeleteFileByURL(peerUrl)
				if err != nil {
					model.WrapMsg(c, model.DELETE_PEER_PACKAGE_FAIL, err)
					return
				}
			}
		}
	}
	err := common.GetPackages()
	if err != nil {
		model.WrapMsg(c, model.DELETE_PEER_PACKAGE_FAIL, err)
		return
	}
	model.WrapMsg(c, model.SUCCESS, nil)
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
