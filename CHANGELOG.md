# v2.2.8
- fix: migrate tool panic while init log
- feat: show uptime
- feat: do not return error when check failed
- feat: check type  when alter table
- feat: add getlog api
- feat: support rename column when alter table
- feat: watch cluster status while deployed by tgz
- fix: restart cluster when change profiles and quotas
- feat: add dryrun when create table
- feat: add cron job to sync distributed table
- fix: check access issue
- fix: scheduled task security protection
- fix: upgrade cause permission denied issue
- feat:add getpartions by table api

# v2.2.7
- dm8 database adapt
- handle map is null in frontend


# v2.2.6
- fix: When deploying a cluster and modifying the cluster configuration, an error will be reported if Profiles and quotas are not filled in
- fix: The cluster installation package type deployed in the old version is not automatically populated, resulting in the problem that the configuration cannot be modified
- feat: support password_double_sha1_hex and password_sha256_hex to encrypt password
- feat: sshpassword can be empty
- feat: login password visiable
- fix: goroutine leak issue 
- feat: use scp instead of sftp
- fix: store failed when persistent is mysql
- feat: sort tasks by updatetime
- fix: task duration always be  0s while persistant is mysql
- fix: select all and only return first 10000 records
- feat: add alter table ttl
- fix: query select panic when type is Nullbale(Float64) and value is null
- fix: sync logic schema schedule when ckman is multiple
- fix: cluster setting cause duplicate logics issue
- fix: update running task to stopped when force shutdown


# v2.2.5
- refactor package management
- add api: kill open session & stop running task
- matching paltform when deploy
- config editable
- fix add node failed over 22.x
- fix tableList Duplication bug
- use gsypt to encode password
- ping cluster use default database
- add `database_atomic_delay_before_drop_table_sec`
- add swagger link and document link
- config logic cluster
- check path non-prefix
- support tgz deployment without root
- users conf with profiles and quotas
- make a cron job to sync logic table schema

# v2.2.4
- delete record when destory cluster && check timeout issue
- do not response such errors when prometheus address is invalid
- handle special characters
- support expert config
- fix AWS S3 expert config required issue
- fix reliaca Status bug
- support postgres as persistent policy
- fix move_factor issue
- adapted for arm64

# v2.2.3
- more messages show in task list
- fix logic cluster config issue

# v2.2.2
- fix time.After leak
- fix json-big in queries
- fix mysql password authenticate
- use tasklist control deploy process
- encrypt nacos password
- fix rebalance issue

# v2.2.1
- fix logic config issue
- rpm install ckman issue
- save query history on server
- secret config issue
- fix sync logic schema issue when replica is true
- determine if each node supports IPv6 issue
- auto sync local table when create logic table failed
- fix connection pool cache issue
- fix some frontend bugs


# v2.2.0
- user config
- prometheus address with each cluster
- support posisitent cluster config to mysql
- interface beauty
- fix schema and replica issue

# v2.1.3
- disabled text_log
- rename `macros.xml` to `host.xml`
- fix check error when upgrade and config
- start ckman by ckman user（just rpm）
- rename dist_table to dist_logic_table
- add delete dist_logic_table api
- sync logic table schema when create new cluster on logic
- remove ensureHosts
- create/alter table with ttl

# v2.1.2
- fix `hdfs_zero_copy` cause clickhouse-server start failed.
- add cluster settings
- add merge_tree config
- remove template and use `custom.xml` in config.d

# v2.1.1
- fix deploy cluster with public key, can't find HOME env issue
- fix deploy cluster with AuthenticateType(save password) issue

# v2.1.0
- newly frontend ui in create cluster
- support storage policy
- fix since v21.6.6.51, install clickhouse will ask for password with default issue

# v2.0.0
- single node online & offline
- do not save sshPassword if necessary
- support public key to deploy and manage cluster
- support normal user to deploy and manage cluster
- support https
- support rolling update
- when cluster is not exist, do not allow to import

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
