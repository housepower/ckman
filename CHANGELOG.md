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