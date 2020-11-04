package deploy

type DeployBase struct {
	Hosts     []string
	Packages  []string
	User      string
	Password  string
	Directory string
}
