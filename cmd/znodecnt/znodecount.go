package main

/*
znodefix  --cluster=abc --config=/etc/ckman/conf/ckman.hjson --node=192.168.110.8 --dryrun
*/

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/housepower/ckman/log"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
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
	zkhosts     = flag.String("h", "127.0.0.1:2181", `zookeeper server hosts`)
	node        = flag.String("n", "/clickhouse", "znode")
	recursive   = flag.Bool("r", false, "show all sub znodes")
	sort_number = flag.Int("s", 0, "return number znode count order by count desc")
	zcnts       ZCntList
)

func init() {
	flag.Parse()
}

func main() {
	log.InitLoggerConsole()
	log.Logger.Infof("znode_count start...")
	log.Logger.Infof("zookeeper service: %s, root znode: %s, recursive: %v, sort_number: %d", *zkhosts, *node, *recursive, *sort_number)
	var hosts []string
	var zkPort int
	for _, zkhost := range strings.Split(*zkhosts, ",") {
		host, port, _ := net.SplitHostPort(zkhost)
		hosts = append(hosts, host)
		zkPort, _ = strconv.Atoi(port)
	}

	service, err := zookeeper.NewZkService(hosts, zkPort)
	if err != nil {
		log.Logger.Fatalf("can't create zookeeper instance:%v", err)
	}

	if err = ZkCount(service); err != nil {
		log.Logger.Fatalf("%v\n", err)
	}
}

func ZkCount(service *zookeeper.ZkService) error {
	cnt, err := count(service, *node)
	if err != nil {
		return err
	}

	if *sort_number == 0 {
		fmt.Printf("%s\t%d\n", *node, cnt)
	} else {
		for _, z := range zcnts {
			fmt.Printf("%s\t%d\n", z.Node, z.Count)
		}
	}

	return nil
}

func count(service *zookeeper.ZkService, znode string) (int32, error) {
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
			if c, err := count(service, znode+"/"+child); err != nil {
				return cnt, err
			} else {
				cnt += c
			}
		}
		cnt++
		if *sort_number > 0 {
			zcnts = append(zcnts, ZCnt{
				Node:  znode,
				Count: cnt,
			})

			sort.Sort(sort.Reverse(zcnts))

			if len(zcnts) > *sort_number {
				zcnts = zcnts[:*sort_number]
			}
		} else {
			if *recursive {
				fmt.Printf("%s\t%d\n", znode, cnt)
			}
		}
	}

	return cnt, nil
}
