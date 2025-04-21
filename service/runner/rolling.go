package runner

import "github.com/housepower/ckman/model"

const (
	FIRST = iota
	SECOND
	THIRD
	END
)

type Rolling struct {
	num    int
	Shards []model.CkShard
}

/*
*
FIRST: 取分片1的副本1  <= 用于验证可以正常启动服务
SECOND: 取分片1其余所有副本,其他分片的前N-1个副本 <= 保留每分片1副本正常运行外，其余所有副本一次性重启
THIRD: 取除分片1之外的所有分片的最后一个副本 <= 剩余的未重启的副本一次性重启
END: 结束
*/
func (r *Rolling) Next() []string {
	switch r.num {
	case FIRST:
		r.num = SECOND
		return []string{r.Shards[0].Replicas[0].Ip}
	case SECOND:
		r.num = THIRD
		nodes := make([]string, 0)
		for i := 0; i < len(r.Shards); i++ {
			if i == 0 {
				for j := 1; j < len(r.Shards[i].Replicas); j++ {
					nodes = append(nodes, r.Shards[i].Replicas[j].Ip)
				}
			} else {
				for j := 0; j < len(r.Shards[i].Replicas)-1; j++ {
					nodes = append(nodes, r.Shards[i].Replicas[j].Ip)
				}
			}
		}
		return nodes
	case THIRD:
		r.num = END
		nodes := make([]string, 0)
		for i := 1; i < len(r.Shards); i++ {
			idx := len(r.Shards[i].Replicas) - 1
			nodes = append(nodes, r.Shards[i].Replicas[idx].Ip)
		}
		return nodes
	case END:
		return nil
	default:
		return nil
	}
}
