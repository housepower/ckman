package clickhouse

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

type LocalFactory struct{}

func (LocalFactory) Create() Archive {
	return &TargetLocal{}
}

type TargetLocal struct {
	ArchiveParams
	local model.ArchiveLocal
}

func (t *TargetLocal) Normalize(params ArchiveParams, req model.ArchiveTableReq) error {
	t.ArchiveParams = params
	t.local = req.Local
	return nil
}

func (t *TargetLocal) Init() error {
	if err := t.InitConns(); err != nil {
		return err
	}
	if err := t.GetSortingInfo(); err != nil {
		return err
	}
	return nil
}

func (t *TargetLocal) Engine(_ string) string {
	return fmt.Sprintf("File(%s)", t.Format)
}

func (t *TargetLocal) Clear() error {
	slotBeg, _ := time.Parse(DateLayout, t.Begin)
	slotEnd, _ := time.Parse(DateLayout, t.End)
	for _, host := range t.Hosts {
		opts := common.SshOptions{
			User:             t.SshUser,
			Password:         t.SshPassword,
			Port:             t.SshPort,
			Host:             host,
			NeedSudo:         t.Needsudo,
			AuthenticateType: t.AuthenticateType,
		}
		for _, table := range t.Tables {
			dir := path.Join(t.local.Path, t.Cluster, t.Database+"."+table)
			t.Dirs = append(t.Dirs, dir)
			cmd := fmt.Sprintf("ls %s", path.Join(dir, fmt.Sprintf("archive_%s", table)))
			out, err := common.RemoteExecute(opts, cmd)
			if err != nil {
				log.Logger.Warnf("cmd %s on %s failed: %v", cmd, host, err)
				continue
			}
			entry := strings.Split(out, "\n")
			for _, e := range entry {
				e = strings.TrimRight(e, "\r")
				name := path.Base(e)
				slot := strings.Split(strings.TrimSuffix(name, t.Suffix), "_")[2] // archive_tableName_slot.suffix
				slotTime, err := time.Parse(SlotTimeFormat, slot)
				if err != nil {
					log.Logger.Errorf("parse time error: %+v", err)
					return errors.Wrap(err, host)
				}
				if (slotTime.After(slotBeg) || slotTime.Equal(slotBeg)) && slotTime.Before(slotEnd) {
					_, err := common.RemoteExecute(opts, fmt.Sprintf("rm -rf %s", e))
					if err != nil {
						return errors.Wrap(err, host)
					}
				}
			}
		}

	}

	return nil
}

func (t *TargetLocal) Export() error {
	var err error
	if err = t.GetAllSlots(); err != nil {
		return err
	}
	t0 := time.Now()

	engines := []string{
		t.Engine(""),
	}
	var lastErr error
	var wg sync.WaitGroup
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
				if err = t.ExportSlot(host, slot.Table, i, slot.SlotBeg, slot.SlotEnd, engines); err != nil {
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

func (t *TargetLocal) Done(fp string) {
	// copy to path
	slotBeg, _ := time.Parse(DateLayout, t.Begin)
	slotEnd, _ := time.Parse(DateLayout, t.End)
	for i, host := range t.Hosts {
		opts := common.SshOptions{
			User:             t.SshUser,
			Password:         t.SshPassword,
			Port:             t.SshPort,
			Host:             host,
			NeedSudo:         t.Needsudo,
			AuthenticateType: t.AuthenticateType,
		}
		for _, table := range t.Tables {
			p := path.Join(fp, "clickhouse", "data", t.Database)
			cmd := fmt.Sprintf("ls %s", p)
			out, err := common.RemoteExecute(opts, cmd)
			if err != nil {
				log.Logger.Errorf("excute command %s[%s] failed: %v", host, cmd, err)
			}
			entry := strings.Split(out, "\n")
			for _, e := range entry {
				name := path.Base(strings.TrimRight(e, "\r"))
				if strings.HasPrefix(name, fmt.Sprintf("archive_%s_", table)) {
					idx := strings.LastIndex(name, "_")
					slot := name[idx+1:]
					slotTime, err := time.Parse(SlotTimeFormat, slot)
					if err != nil {
						log.Logger.Errorf("parse time error: %+v", err)
						continue
					}
					if (slotTime.After(slotBeg) || slotTime.Equal(slotBeg)) && slotTime.Before(slotEnd) {
						dir := path.Join(t.local.Path, t.Cluster, t.Database+"."+table, fmt.Sprintf("shard_%d_%s", i, host), name)
						cmds := []string{
							fmt.Sprintf("mkdir -p %s", dir),
							fmt.Sprintf("cp -prfL %s/data.%s %s/", path.Join(p, name), t.Format, dir),
						}
						_, err = common.RemoteExecute(opts, strings.Join(cmds, ";"))
						if err != nil {
							log.Logger.Errorf("excute command %s failed: %v", host, err)
						}
					}
				}
			}
		}
	}
	for _, host := range t.Hosts {
		conn := common.GetConnection(host)
		for _, tmpTbl := range t.TmpTables {
			query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTbl)
			conn.Exec(context.Background(), query)
		}
	}

	target := t.Hosts[0]
	var wg sync.WaitGroup
	for i := 1; i < len(t.Hosts); i++ {
		for _, table := range t.Tables {
			i := i
			wg.Add(1)
			common.Pool.Submit(func() {
				defer wg.Done()
				dir := path.Join(t.local.Path, t.Cluster, t.Database+"."+table, fmt.Sprintf("shard_%d_%s", i, t.Hosts[i]))
				cmds := []string{
					fmt.Sprintf(`rsync -e "ssh -o StrictHostKeyChecking=false" -avp %s %s:%s`, dir, target, path.Dir(dir)+"/"),
					fmt.Sprintf(`rm -fr %s`, dir),
				}
				opts := common.SshOptions{
					User:             t.SshUser,
					Password:         t.SshPassword,
					Port:             t.SshPort,
					Host:             t.Hosts[i],
					NeedSudo:         true,
					AuthenticateType: t.AuthenticateType,
				}
				for _, cmd := range cmds {
					if _, err := common.RemoteExecute(opts, cmd); err != nil {
						log.Logger.Errorf("execute %s failed: %v", cmd, err)
						return
					}
				}
			})
		}
	}
	wg.Wait()
}
