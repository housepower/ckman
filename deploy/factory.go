package deploy

// 工厂方法
type DeployFactory interface {
	Create() Deploy
}