package legacyjson

// NewWriter 创建一个 legacy 后端实例，等待被填入数据并 dump 到指定 JSON/YAML 文件。
// 与 NewReader 的差异只在语义；底层实现完全相同。
func NewWriter(cfg LocalConfig) (*LocalPersistent, error) {
	return NewReader(cfg)
}
