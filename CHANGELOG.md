
# v2.0.0.beta1
- support logic cluster
- monitor `outstanding_requests` and `watch_count` in zookeeper
- use `0.0.0.0` instead of `::` if node doesn't support ipv6
- do not replace ckman.yaml when upgrade ckman
- metadata manage
- get slow_session with condition
- fix ensureHosts issue
- encript password when write cluster config into disk 


# v1.3.7
- fix ensureHosts issue
- fix wrap error issue

# v1.3.6
- fix can't create cluster issue([#93](https://github.com/housepower/ckman/issues/93))
- fix status always show green on Manage page
- fix upgrade cluster cause config panic issue
- Interface beauty
- show disk information on ckman.

# v1.3.5
- fix `database is closed` issue
- if mode is import, get cluster info from clickhouse every time

# v1.3.4
- fix json unmarshal of long request issue
- fix 'URL is not allowed in config.xml' since 21.4
- still create table successfully if shard is avaliable
- Allow multiple nodes to be added once
- remove hostnames in clusters.json
- support query_log ttl

# v1.3.3
- detach macros.xml from metrika.xml
- filter metrics by cluster name
- remove loader

# v1.3.2
- show version on web page
- support routine, more faster to operate cluster
- support internationalize 
- not allow to modify isReplica
- write clusters.json format
- fix runtime error when get zk version
- make ssh port configurable([#63](https://github.com/housepower/ckman/issues/63))
- do not report error when system.query_log is not exist([#60](https://github.com/housepower/ckman/issues/60))
- when node server is disconnect, we can also delete it
- only start node when status is red


# v1.3.1
- The node health is displayed on the Manage interface, which is connected for Green, disconnected for RED
- Error prompts to automatically disappear later
- Cleaning ZooKeeper information when destroy cluster, delete node, delete table. ([#42](https://github.com/housepower/ckman/issues/42))
- Rebalancer not only support default database ([#45](https://github.com/housepower/ckman/issues/45))
- Repairing the replicated table status, the node does not exist ([#27](https://github.com/housepower/ckman/issues/27))
- Repair Slow Session No Display Questions ([#32](https://github.com/housepower/ckman/issues/32))
- When the node is deleted, when the Shard number is not the last one, and the node is alone in a Shard, it is not allowed to be deleted.
- Simplify the compilation step, the frontend is packaged into `dist`, no need to manually compile


# v1.3.0
- add http port in cluster config
- fix get rows in table metrics
- nacos default disabled when first start
- do not restart cluster when add or delete node
- fix upgrade bug ([#39](https://github.com/housepower/ckman/issues/39))
- handle 500 code in Gin

# v1.2.9
- fix get table metric bugs([#22](https://github.com/housepower/ckman/issues/22))
- fix create table with incorrect zookeeper path
- change listen host `0.0.0.0` to `::`
    - with `0.0.0.0`, you can't connect clickhouse-server with clickhouse-client when use localhost
    - If ipv6 is not in your machine, `::` will return error when you start clickhouse-server ,then `0.0.0.0` will work.

# v1.2.8
- add API PingCluster
- add API PurgerTables
- add API ArchiveToHDFS
- add API GetCluster
- modify WrapMsg to unify error response
- escape column names, more debug log
- fixed DeployController.DeployCk to store clusters

# v1.2.7
- Fix bugs in the password of Clickhouse that cannot be recognized by special characters such as `#`, `+`, `&` etc;
- Since the default user is a reserved user in the Clickhouse, from v1.2.7, default user deployment cluster is no longer supported;
- Zookeeeper status port can be configured from the interface to monitor the performance of zookeeeper. The default port is 8080
- Supported launch from container ([#7](https://github.com/housepower/ckman/issues/7))
- When the cluster is in import mode, all operations on the cluster are disabled on the interface, including adding and deleting nodes, upgrading the cluster, starting and stopping the cluster, and rebalance;
- For security reasons, Access to swagger documents is no longer supported by default

# v1.2.6
- Fix upgrade cluster bug
- Amend frontend word 'Cluter' to 'Cluster'
- Intercept token from unified portal, this feature only for eoi product, If you don't use eoi, you can ignore it， do not affect usage effect.

# v1.2.5
- the first release version.