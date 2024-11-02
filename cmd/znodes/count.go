package znodes

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/service/zookeeper"
)

type ZCnt struct {
	Node  string
	Count int32
}

type ZCntList []ZCnt

func (v ZCntList) Len() int           { return len(v) }
func (v ZCntList) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v ZCntList) Less(i, j int) bool { return v[i].Count < v[j].Count }

var (
	zcnts ZCntList
)

type ZCntOpts struct {
	Zkhosts    string
	Path       string
	Recursive  bool
	SortNumber int
}

func ZCntHandle(opts ZCntOpts) {
	log.Logger.Infof("znode_count start...")
	log.Logger.Infof("zookeeper service: %s, root znode: %s, recursive: %v, sort_number: %d", opts.Zkhosts, opts.Path, opts.Recursive, opts.SortNumber)
	var hosts []string
	var zkPort int
	for _, zkhost := range strings.Split(opts.Zkhosts, ",") {
		host, port, _ := net.SplitHostPort(zkhost)
		hosts = append(hosts, host)
		zkPort, _ = strconv.Atoi(port)
	}

	service, err := zookeeper.NewZkService(hosts, zkPort, 300)
	if err != nil {
		log.Logger.Fatalf("can't create zookeeper instance:%v", err)
	}

	if err = ZkCount(service, opts); err != nil {
		log.Logger.Fatalf("%v\n", err)
	}
}

func ZkCount(service *zookeeper.ZkService, opts ZCntOpts) error {
	cnt, err := count(service, opts.Path, opts)
	if err != nil {
		return err
	}

	if opts.SortNumber == 0 {
		fmt.Printf("%s\t%d\n", opts.Path, cnt)
	} else {
		for _, z := range zcnts {
			fmt.Printf("%s\t%d\n", z.Node, z.Count)
		}
	}

	return nil
}

func count(service *zookeeper.ZkService, znode string, opts ZCntOpts) (int32, error) {
	var cnt int32
	_, n, err := service.Conn.Get(znode)
	if err != nil {
		return cnt, err
	}
	if n.NumChildren == 0 {
		cnt++
	} else {
		children, _, _ := service.Conn.Children(znode)
		for _, child := range children {
			if c, err := count(service, znode+"/"+child, opts); err != nil {
				return cnt, err
			} else {
				cnt += c
			}
		}
		cnt++
		if opts.SortNumber > 0 {
			zcnts = append(zcnts, ZCnt{
				Node:  znode,
				Count: cnt,
			})

			sort.Sort(sort.Reverse(zcnts))

			if len(zcnts) > opts.SortNumber {
				zcnts = zcnts[:opts.SortNumber]
			}
		} else {
			if opts.Recursive {
				fmt.Printf("%s\t%d\n", znode, cnt)
			}
		}
	}

	return cnt, nil
}
