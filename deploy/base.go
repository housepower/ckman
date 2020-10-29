package deploy

// Hosts, 指定需要安装的主机
// Packages, 指定安装该组件需要用到的包
// User、Password, 指定登陆主机需要用到的用户名密码
// Directory, 指定安装目录
type DeployBase struct {
	Hosts     []string
	Packages  []string
	User      string
	Password  string
	Directory string
}
