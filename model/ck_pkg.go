package model

type PkgInfo struct {
	Version string `json:"version,omitempty"`
	PkgType string `json:"pkgType,omitempty"`
	PkgName string `json:"pkgName,omitempty"`
}
