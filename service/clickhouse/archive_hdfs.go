package clickhouse

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

type HdfsFactory struct{}

func (HdfsFactory) Create() Archive {
	return &TargetHdfs{}
}

type TargetHdfs struct {
	ArchiveParams
	hdfs model.ArchiveHdfs
	hc   *hdfs.Client
}

func (t *TargetHdfs) Normalize(params ArchiveParams, req model.ArchiveTableReq) error {
	t.ArchiveParams = params
	t.hdfs = req.HDFS
	if t.hdfs.User == "" {
		t.hdfs.User = HdfsUserDefault
	}
	return nil
}

func (t *TargetHdfs) Init() error {
	var err error
	if err = t.InitConns(); err != nil {
		return err
	}

	ops := hdfs.ClientOptions{
		Addresses: []string{t.hdfs.Addr},
		User:      t.hdfs.User,
	}

	if t.hc, err = hdfs.NewClient(ops); err != nil {
		err = errors.Wrapf(err, "")
		log.Logger.Errorf("got error %+v", err)
		return err
	}

	if err := t.GetSortingInfo(); err != nil {
		return err
	}
	return nil
}

func (t *TargetHdfs) Engine(fp string) string {
	return fmt.Sprintf("HDFS('hdfs://%s', '%s')", path.Join(t.hdfs.Addr, fp), t.Format)
}

func (t *TargetHdfs) Clear() error {
	var err error

	slotBeg, _ := time.Parse(DateLayout, t.Begin)
	slotEnd, _ := time.Parse(DateLayout, t.End)
	for _, table := range t.Tables {
		dir := path.Join(t.hdfs.Dir, t.Cluster, t.Database+"."+table)
		t.Dirs = append(t.Dirs, dir)
		err = t.hc.MkdirAll(dir, 0777)
		if err != nil {
			log.Logger.Errorf("got error %+v", err)
			return err
		}
		var fileList []os.FileInfo
		if fileList, err = t.hc.ReadDir(dir); err != nil {
			err = errors.Wrapf(err, "")
			log.Logger.Errorf("got error %+v", err)
			return err
		}
		for _, file := range fileList {
			name := file.Name()
			if !strings.Contains(name, t.Suffix) {
				continue
			}

			slot := strings.Split(strings.TrimSuffix(name, t.Suffix), "_")[3] //shard_%d_host_slot.suffix
			slotTime, err := time.Parse(SlotTimeFormat, slot)
			if err != nil {
				log.Logger.Errorf("parse time error: %+v", err)
				return err
			}
			if (slotTime.After(slotBeg) || slotTime.Equal(slotBeg)) && slotTime.Before(slotEnd) {
				filePath := path.Join(dir, name)
				if err = t.hc.Remove(filePath); err != nil {
					err = errors.Wrapf(err, "")
					log.Logger.Errorf("got error %+v", err)
					return err
				}
			}
		}
		log.Logger.Infof("cleared hdfs directory %s", dir)
	}

	return nil
}

func (t *TargetHdfs) Export() error {
	var err error
	if err = t.GetAllSlots(); err != nil {
		return err
	}
	t0 := time.Now()
	var wg sync.WaitGroup
	var lastErr error
	for i, host := range t.Hosts {
		wg.Add(1)
		i := i
		host := host
		go func() {
			defer wg.Done()
			for _, slot := range t.Slots {
				if slot.Host != host {
					continue
				}
				engines := make([]string, 0)
				for _, dir := range t.Dirs {
					fp := filepath.Join(dir, fmt.Sprintf("shard_%d_%s", i, host), fmt.Sprintf("archive_%s_%v", slot.Table, slot.SlotBeg.Format(SlotTimeFormat)), "data"+t.Suffix)
					err = t.hc.MkdirAll(path.Dir(fp), 0777)
					if err != nil {
						lastErr = err
					}
					engines = append(engines, t.Engine(fp))
				}
				if err := t.ExportSlot(host, slot.Table, i, slot.SlotBeg, slot.SlotEnd, engines); err != nil {
					lastErr = err
				}
			}
		}()
	}
	wg.Wait()

	if lastErr != nil {
		return err
	}

	du := uint64(time.Since(t0).Seconds())
	size := atomic.LoadUint64(&t.EstSize)
	msg := fmt.Sprintf("exported %d bytes in %d seconds", size, du)
	if du != 0 {
		msg += fmt.Sprintf(", %d bytes/s", size/du)
	}
	log.Logger.Infof(msg)

	return nil
}

func (t *TargetHdfs) Done(_ string) {
	for _, host := range t.Hosts {
		conn := common.GetConnection(host)
		for _, tmpTbl := range t.TmpTables {
			query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTbl)
			conn.Exec(context.Background(), query)
		}
	}

	t.hc.Close()
}
