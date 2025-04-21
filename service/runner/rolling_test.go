package runner

import (
	"fmt"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestRolling1(t *testing.T) {
	r := Rolling{
		Shards: []model.CkShard{
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.1",
						HostName: "host1",
					},
					{
						Ip:       "192.168.0.2",
						HostName: "host2",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.3",
						HostName: "host3",
					},
					{
						Ip:       "192.168.0.4",
						HostName: "host4",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.5",
						HostName: "host5",
					},
					{
						Ip:       "192.168.0.6",
						HostName: "host6",
					},
				},
			},
		},
	}

	for nodes := r.Next(); nodes != nil; nodes = r.Next() {
		fmt.Println(nodes)
	}
}

func TestRolling2(t *testing.T) {
	r := Rolling{
		Shards: []model.CkShard{
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.1",
						HostName: "host1",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.2",
						HostName: "host2",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.3",
						HostName: "host3",
					},
				},
			},
		},
	}

	for nodes := r.Next(); nodes != nil; nodes = r.Next() {
		fmt.Println(nodes)
	}
}

func TestRolling3(t *testing.T) {
	r := Rolling{
		Shards: []model.CkShard{
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.1",
						HostName: "host1",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.2",
						HostName: "host2",
					},
					{
						Ip:       "192.168.0.3",
						HostName: "host3",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.4",
						HostName: "host4",
					},
					{
						Ip:       "192.168.0.5",
						HostName: "host5",
					},
					{
						Ip:       "192.168.0.6",
						HostName: "host6",
					},
				},
			},
		},
	}

	for nodes := r.Next(); nodes != nil; nodes = r.Next() {
		fmt.Println(nodes)
	}
}

func TestRolling4(t *testing.T) {
	r := Rolling{
		Shards: []model.CkShard{
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.1",
						HostName: "host1",
					},
					{
						Ip:       "192.168.0.2",
						HostName: "host2",
					},
					{
						Ip:       "192.168.0.3",
						HostName: "host3",
					},
				},
			},
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.4",
						HostName: "host4",
					},
					{
						Ip:       "192.168.0.5",
						HostName: "host5",
					},
					{
						Ip:       "192.168.0.6",
						HostName: "host6",
					},
				},
			},
		},
	}

	for nodes := r.Next(); nodes != nil; nodes = r.Next() {
		fmt.Println(nodes)
	}
}

func TestRolling5(t *testing.T) {
	r := Rolling{
		Shards: []model.CkShard{
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.1",
						HostName: "host1",
					},
				},
			},
		},
	}

	for nodes := r.Next(); nodes != nil; nodes = r.Next() {
		fmt.Println(nodes)
	}
}

func TestRolling6(t *testing.T) {
	r := Rolling{
		Shards: []model.CkShard{
			{
				Replicas: []model.CkReplica{
					{
						Ip:       "192.168.0.1",
						HostName: "host1",
					},
					{
						Ip:       "192.168.0.2",
						HostName: "host2",
					},
					{
						Ip:       "192.168.0.3",
						HostName: "host3",
					},
				},
			},
		},
	}

	for nodes := r.Next(); nodes != nil; nodes = r.Next() {
		fmt.Println(nodes)
	}
}
