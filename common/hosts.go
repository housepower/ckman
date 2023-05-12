package common

// import (
// 	"github.com/pkg/errors"
// 	"github.com/txn2/txeh"
// )

// func NewHosts(readFile, writeFile string) (*txeh.Hosts, error) {
// 	conf := &txeh.HostsConfig{
// 		ReadFilePath:  readFile,
// 		WriteFilePath: writeFile,
// 	}

// 	return txeh.NewHosts(conf)
// }

// func AddHost(h *txeh.Hosts, address, host string) error {
// 	if h != nil && address != "" && host != "" {
// 		h.AddHost(address, host)
// 		return nil
// 	} else {
// 		return errors.Errorf("parameters invalid")
// 	}
// }

// func AddHosts(h *txeh.Hosts, addresses, hosts []string) error {
// 	addressNum := len(addresses)
// 	hostNum := len(hosts)

// 	if h != nil && addressNum != 0 && hostNum != 0 && addressNum == hostNum {
// 		for i := 0; i < addressNum; i++ {
// 			h.AddHost(addresses[i], hosts[i])
// 		}
// 		return nil
// 	} else {
// 		return errors.Errorf("parameters invalid")
// 	}
// }

// func RemoveHost(h *txeh.Hosts, host string) error {
// 	if h != nil && host != "" {
// 		h.RemoveHost(host)
// 		return nil
// 	} else {
// 		return errors.Errorf("parameters invalid")
// 	}
// }

// func RemoveHosts(h *txeh.Hosts, hosts []string) error {
// 	if h != nil && len(hosts) != 0 {
// 		h.RemoveHosts(hosts)
// 		return nil
// 	} else {
// 		return errors.Errorf("parameters invalid")
// 	}
// }

// func Save(h *txeh.Hosts) error {
// 	return h.Save()
// }
