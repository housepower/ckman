package legacyjson

// NewWriter 创建一个 legacy 后端实例，等待被填入数据并 dump 到指定 JSON/YAML 文件。
// 底层实现等同于 NewReader：Init 时如果目标路径已有同名文件会被读入。
// 调用方（如 dump-to-json）若需要"覆盖式写出"，应当先确保目标路径不存在或
// 显式接受合并语义。
func NewWriter(cfg LocalConfig) (*LocalPersistent, error) {
	return NewReader(cfg)
}
