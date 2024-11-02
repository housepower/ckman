package zookeeper

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/housepower/ckman/repository"

	"github.com/go-zookeeper/zk"
	"github.com/housepower/ckman/model"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

var ZkServiceCache *cache.Cache

type ZkService struct {
	ZkNodes        []string
	ZkPort         int
	SessionTimeout int
	Conn           *zk.Conn
	Event          <-chan zk.Event
}

func NewZkService(nodes []string, port int, sessionTimeout int) (*ZkService, error) {
	zkService := &ZkService{
		ZkNodes:        nodes,
		ZkPort:         port,
		SessionTimeout: sessionTimeout,
	}

	servers := make([]string, len(nodes))
	for index, node := range nodes {
		servers[index] = net.JoinHostPort(node, fmt.Sprint(port))
	}

	c, e, err := zk.Connect(servers, time.Duration(sessionTimeout)*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	zkService.Conn = c
	zkService.Event = e

	return zkService, nil
}

func (z *ZkService) Reconnect() error {

	servers := make([]string, len(z.ZkNodes))
	for index, node := range z.ZkNodes {
		servers[index] = net.JoinHostPort(node, fmt.Sprint(z.ZkPort))
	}
	c, e, err := zk.Connect(servers, time.Duration(z.SessionTimeout)*time.Second)
	if err != nil {
		return err
	}

	z.Conn = c
	z.Event = e
	return nil
}

func GetZkService(clusterName string) (*ZkService, error) {
	zkService, ok := ZkServiceCache.Get(clusterName)
	if ok {
		return zkService.(*ZkService), nil
	} else {
		conf, err := repository.Ps.GetClusterbyName(clusterName)
		if err == nil {
			nodes, port := GetZkInfo(&conf)
			service, err := NewZkService(nodes, port, 300)
			if err != nil {
				return nil, err
			}
			ZkServiceCache.SetDefault(clusterName, service)
			return service, nil
		} else {
			return nil, errors.Errorf("can't find cluster %s zookeeper service", clusterName)
		}
	}
}

func (z *ZkService) DeleteAll(node string) (err error) {
	children, stat, err := z.Conn.Children(node)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	} else if err != nil {
		err = errors.Wrap(err, "delete zk node: ")
		return
	}

	for _, child := range children {
		if err = z.DeleteAll(path.Join(node, child)); err != nil {
			err = errors.Wrap(err, "delete zk node: ")
			return
		}
	}

	return z.Conn.Delete(node, stat.Version)
}

func (z *ZkService) Delete(node string) (err error) {
	_, stat, err := z.Conn.Get(node)
	if err != nil {
		return
	}
	return z.Conn.Delete(node, stat.Version)
}

func (z *ZkService) DeletePathUntilNode(path, endNode string) error {
	ok, _, _ := z.Conn.Exists(path)
	if !ok {
		return nil
	}

	for {
		node := filepath.Base(path)
		parent := filepath.Dir(path)
		if node == endNode {
			return z.DeleteAll(path)
		}
		if parent == "/clickhouse/tables" {
			return nil
		}
		path = parent
	}
}

func (z *ZkService) CleanZoopath(conf model.CKManClickHouseConfig, clusterName, target string, dryrun bool) error {
	root := fmt.Sprintf("/clickhouse/tables/%s", clusterName)
	return clean(z, root, target, dryrun)
}

func clean(svr *ZkService, znode, target string, dryrun bool) error {
	_, stat, err := svr.Conn.Get(znode)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, znode)
	}
	base := path.Base(znode)
	if base == target {
		if dryrun {
			fmt.Println(znode)
		} else {
			err = svr.DeleteAll(znode)
			if err != nil {
				fmt.Printf("znode %s delete failed: %v\n", znode, err)
			} else {
				fmt.Printf("znode %s delete success\n", znode)
			}
		}
	} else if stat.NumChildren > 0 {
		children, _, err := svr.Conn.Children(znode)
		if err != nil {
			return errors.Wrap(err, znode)
		}

		for _, child := range children {
			subnode := path.Join(znode, child)
			err := clean(svr, subnode, target, dryrun)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ZkMetric(host string, port int, metric string) ([]byte, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(metric))
	if err != nil {
		return nil, err
	}
	var b []byte
	for {
		buf := [8192]byte{}
		n, err := conn.Read(buf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n == 0 {
			break
		}
		b = append(b, buf[:n]...)
	}
	resp := make(map[string]interface{})
	lines := strings.Split(string(b), "\n")
	re := regexp.MustCompile(`zk_(\w+)\s+(.*)`)
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) >= 3 {
			resp[matches[1]] = matches[2]
		}
	}
	if len(resp) == 0 {
		return b, nil
	}
	return json.Marshal(resp)
}

func GetZkInfo(conf *model.CKManClickHouseConfig) ([]string, int) {
	var nodes []string
	var port int
	if conf.Keeper == model.ClickhouseKeeper {
		nodes = conf.KeeperConf.KeeperNodes
		port = conf.KeeperConf.TcpPort
	} else {
		nodes = conf.ZkNodes
		port = conf.ZkPort
	}
	return nodes, port
}
