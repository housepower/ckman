package legacyjson

// 不再调用 RegistePersistent —— legacyjson 仅作为迁移读源 / dump 写目标，
// 不能被 persistent_policy 配置选中。
//
// 启动期迁移：repository/sqlite/migrate.go 直接调 NewReader(cfg) 构造实例。
// dump-to-json 工具：cmd/dumpjson 直接调 NewWriter(cfg) 构造实例。

// NewReader 打开（或创建空）legacy JSON / YAML 文件，提供完整的 PersistentMgr 读路径。
// 写路径同样保留以兼容 cmd/migrate 反向场景（dump-to-json）。
func NewReader(cfg LocalConfig) (*LocalPersistent, error) {
	lp := NewLocalPersistent()
	if err := lp.Init(cfg); err != nil {
		return nil, err
	}
	return lp, nil
}
