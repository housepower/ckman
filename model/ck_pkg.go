package model

type PkgInfo struct {
	Version string `json:"version,omitempty"`
	PkgType string `json:"pkgType,omitempty"`
	PkgName string `json:"pkgName,omitempty"`
	// Server  bool
	// Common  bool
	// Client  bool
	// Keeper  bool
	// Active  bool
}
