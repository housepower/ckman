# 部署依赖
- `prometheus`(非必需)
- `node_exporter`(非必需)
- `nacos`(>`1.4`)(非必需)
- `zookeeper`(>`3.6.0`, 推荐 )
- `mysql` (当持久化策略设置为`mysql`时必需)

# 监控配置(可选，不影响ckman核心功能)

## zookeeper监控配置

`zookeeper`集群是`clickhouse`实现分布式集群的重要组件，需要提前搭建好。由于`clickhouse`数据量极大，避免给`zookeeper`带来太大的压力，最好给`clickhouse`单独部署一套集群，不要和其他业务公用。

本文默认`zookeeper`集群已经搭建完成。我们需要在zk各节点的`zoo.cfg`配置文件里加上如下配置：

```ini
metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider
metricsProvider.httpPort=7000  #暴露给promethues的监控端口
admin.enableServer=true
admin.serverPort=8080   #暴露给四字命令如mntr等的监控端口，3.5.0以上版本支持
```

然后依次重启`zookeeper`各个节点。

## clickhouse监控配置

`ckman`部署的`clickhouse`集群默认监听了`9363`端口上报`metric`给`prometheus`，因此无需做任何配置。

如果集群是导入的，请确保`/etc/clickhouse-server/config.xml`中有以下配置内容：

```xml
<!-- Serve endpoint for Prometheus monitoring. -->
    <!--
        endpoint - mertics path (relative to root, statring with "/")
        port - port to setup server. If not defined or 0 than http_port used
        metrics - send data from table system.metrics
        events - send data from table system.events
        asynchronous_metrics - send data from table system.asynchronous_metrics
        status_info - send data from different component from CH, ex: Dictionaries status
    -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>

        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <status_info>true</status_info>
    </prometheus>
```



## node监控配置

`node_exporter`是用来监控`clickhouse`节点所在机器的一些系统指标的一款工具，因此需要安装在`ck`节点所在的机器，默认监听`9100`端口。

安装步骤略。

## promethues配置

```yaml
- job_name: 'node_exporter'
  scrape_interval: 10s
  static_configs:
  - targets: ['192.168.0.1:9100', '192.168.0.2:9100', '192.168.0.3:9100', '192.168.0.4:9100']
 
- job_name: 'clickhouse'
  scrape_interval: 10s
  static_configs:
  - targets: ['192.168.0.1:9363', '192.168.0.2:9363', '192.168.0.3:9363', '192.168.0.4:9363']
 
- job_name: 'zookeeper'
  scrape_interval: 10s
  static_configs:
  - targets: ['192.168.0.1:7070', '192.168.0.2:7070', '192.168.0.3:7070']
```

# 安装步骤
## rpm安装
### 安装

`rpm`安装直接使用命令安装即可：

```bash
rpm -ivh ckman-1.3.1.x86_64.rpm
```

安装完成后，在`/etc/ckman`目录下，会生成工作目录（日志和配置文件等都在该目录下）。

### 启动

`rpm`方式安装的`ckman`有两种启动方式：

#### 方式一：

```bash
/usr/local/bin/ckman -c=/etc/ckman/conf/ckman.yaml -p=/run/ckman/ckman.pid -l=/var/log/ckman/ckman.log -d
```

#### 方式二：

```bash
systemctl start ckman
```

## tar.gz包安装

### 安装

可以在任意目录进行安装。安装方式为直接解压安装包即可。

```bash
tar -xzvf ckman-1.3.1-210428.Linux.x86_64.tar.gz
```

### 启动

进入`ckman`的工作目录，执行：

```bash
cd ckman
bin/start
```

启动之后，在浏览器输入 http://localhost:8808  跳出如下界面，说明启动成功：

![image-20210305134653422](../img/image-20210305134653422.png)

`ckman`默认的登录用户为`ckman`，密码为`Ckman123456!`。

## docker启动

从`v1.2.7`版本开始，`ckman`支持从`docker`镜像启动。启动命令如下所示：

```bash
docker run -itd -p 8808:8808 --restart unless-stopped --name ckman quay.io/housepower/ckman:latest
```

但是需要注意的是，搭建`promethues`和`nacos`并不属于`ckman`程序自身的范畴，因此，从容器启动`ckman`默认是关闭`nacos`的，且前台`Overview`监控不会正常显示。

如果想自己配置`nacos`和`prometheus`，可以进入容器自行配置。
