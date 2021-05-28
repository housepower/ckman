`ckman`支持`rpm`安装和`tar.gz`安装，所以升级也针对这两种安装方式做了区分。

# rpm升级
从`github`上下载最新版的`ckman`[安装包](https://github.com/housepower/ckman/releases)。
停止`ckman`服务：
```bash
systemctl stop ckman
```
升级`rpm`包：
```bash
rpm --force -Uvh ckman-x.x.x-x86_64.rpm
```
注意`rpm`升级会覆盖掉原有配置文件, 所以需要将原来的配置替换会去：
```bash
cp /etc/ckman/conf/ckman.yaml.rpmsave /etc/ckman/conf/ckman.yaml
```
重新启动`ckman`：
```bash
systemctl start ckman
```
# tar.gz升级
从`github`上下载最新版的`ckman`[安装包](https://github.com/housepower/ckman/releases)。
进入到当前版本`ckman`的工作目录，停止`ckman`服务：
```bash
bin/stop
```
备份配置文件：
```bash
cp conf/ckman.yaml conf/ckman.yaml.last
```
解压最新下载的安装包覆盖掉旧版本的安装目录：
```bash
tar -xzvf ckman-x.x.x-YYDDMM.Linux.x86_64.tar.gz -C ${WORKDIR}
```
替换配置文件：
```bash
cp conf/ckman.yaml.last conf/ckman.yaml
```
重新启动`ckman`服务：
```bash
bin/start
```
需要注意的是，由于`tar.gz`安装方式可以自行指定工作目录，如果新版本安装位置与旧版本不同，需要将`conf`目录下的`clusters.json`拷贝到新版本的工作目录下，否则无法加载集群信息。