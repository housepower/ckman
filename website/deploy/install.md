# 安装

CKMAN 提供三种发行包：`tar.gz`、`rpm`、`deb`，以及 Docker 镜像。

## 部署依赖

::: tip
下列依赖**除 zookeeper 外都不影响 CKMAN 自身启动**，按需配置即可。
:::

| 组件 | 必需 | 版本要求 | 用途 |
| --- | --- | --- | --- |
| ZooKeeper | 是 | ≥ 3.6.0（推荐） | ClickHouse 副本与分布式 DDL 协调 |
| Nacos | 否 | ≥ 1.4 | 多实例部署的服务发现（参见[高可用部署](/deploy/high-availability)） |
| MySQL / PostgreSQL / DM8 | 否 | — | `persistent_policy` 非 `local` 时必需 |
| Prometheus | 否 | 任意 | **可选**——CKMAN 监控默认直读 ClickHouse 系统表，不依赖 Prometheus。需要长期存储 / 告警时再接入 |
| node_exporter | 否 | 任意 | 仅接入 Prometheus 时需要（默认 9100 端口） |

## RPM 安装

```bash
sudo rpm -ivh ckman-x.x.x.x86_64.rpm
```

工作目录位于 `/etc/ckman/`（日志和配置都在这里）。

启动方式两选一：

```bash
# 方式 1：systemd
sudo systemctl start ckman
sudo systemctl enable ckman

# 方式 2：直接运行
/usr/local/bin/ckman -c=/etc/ckman/conf/ckman.hjson -p=/run/ckman/ckman.pid -l=/var/log/ckman/ckman.log -d
```

## tar.gz 安装

可以在任意目录解压使用：

```bash
tar -xzvf ckman-x.x.x-YYDDMM.Linux.x86_64.tar.gz
cd ckman
bin/start
```

启动后浏览器访问 `http://<host>:8808`。

## DEB 安装

```bash
sudo dpkg -i ckman_x.x.x_amd64.deb
sudo systemctl start ckman
```

## Docker 启动

```bash
docker run -itd \
  -p 8808:8808 \
  --restart unless-stopped \
  --name ckman \
  quay.io/housepower/ckman:latest
```

::: tip
Docker 启动默认不开启 Nacos。监控数据直读 ClickHouse 系统表，**Overview 默认即可显示**，不需要额外配置。需要自定义参数时进入容器修改 `/etc/ckman/conf/ckman.hjson`。
:::

## Kubernetes 部署

适合云原生 / 内部容器平台环境。基于官方镜像 `quay.io/housepower/ckman:latest`，按照下面四份 YAML 部署即可。

::: tip 多副本前提
单副本 K8s 部署使用 `local` 持久化策略即可。如果要起多副本，参考[高可用部署](/deploy/high-availability)，**必须**配置 `mysql` / `postgres` / `dm8` 持久层与 Nacos，否则副本间数据不一致。
:::

### Namespace（可选）

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: ckman
```

### ConfigMap：ckman.hjson

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ckman-config
  namespace: ckman
data:
  ckman.hjson: |
    {
      "server": {
        "port": 8808,
        "https": false,
        "session_timeout": 3600,
        "persistent_policy": "local",
        "task_interval": 5,
        "metric": true,
        "metric_path": "/metrics"
      },
      "log": {
        "level": "INFO",
        "max_count": 5,
        "max_size": 10,
        "max_age": 10
      },
      "clickhouse": {
        "max_open_conns": 10,
        "max_idle_conns": 2,
        "conn_max_idle_time": 10
      },
      "cron": {
        "sync_logic_schema":    "0 * * * * ?",
        "watch_cluster_status": "0 */3 * * * ?",
        "sync_dist_schema":     "30 */10 * * * ?"
      },
      "nacos": {
        "enabled": false
      }
    }
```

::: tip 多副本 + 数据库后端
如果改成 mysql/postgres 持久层，把 `persistent_policy` 改成对应值，并在同一 ConfigMap 里加 `persistent_config` 段；数据库密码建议放到下文 Secret 中再用 `envFrom` 注入或者用 `ENC()` 加密。
:::

### PersistentVolumeClaim：持久化日志与本地配置

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ckman-data
  namespace: ckman
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  # storageClassName: <你的 SC，按需指定>
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckman
  namespace: ckman
  labels:
    app: ckman
spec:
  replicas: 1                   # 多副本前请先准备好数据库后端 + Nacos
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: ckman
  template:
    metadata:
      labels:
        app: ckman
    spec:
      containers:
        - name: ckman
          image: quay.io/housepower/ckman:latest
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8808
          env:
            - name: HOST_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
          # 如果启用 Nacos，把 NACOS_HOST 也注入
          # - name: NACOS_HOST
          #   value: "nacos-headless.nacos.svc:8848"
          readinessProbe:
            httpGet:
              path: /api/v1/version
              port: 8808
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /api/v1/version
              port: 8808
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 1Gi
          volumeMounts:
            - name: config
              mountPath: /etc/ckman/conf/ckman.hjson
              subPath: ckman.hjson
            - name: data
              mountPath: /etc/ckman/conf       # 含 clusters.db / password 等
            - name: logs
              mountPath: /var/log/ckman
      volumes:
        - name: config
          configMap:
            name: ckman-config
        - name: data
          persistentVolumeClaim:
            claimName: ckman-data
        - name: logs
          emptyDir: {}
```

::: warning 关于 SSH 私钥
CKMAN 通过 SSH 部署/管理 ClickHouse 节点，公钥认证场景下需要 `.ssh/id_rsa` 文件。生产环境建议把私钥放入 Secret 并挂载到 `/etc/ckman/conf/.ssh/id_rsa`，权限设为 `0600`：

```yaml
volumeMounts:
  - name: ssh-key
    mountPath: /etc/ckman/conf/.ssh
    readOnly: true
volumes:
  - name: ssh-key
    secret:
      secretName: ckman-ssh-key
      defaultMode: 0600
```
:::

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ckman
  namespace: ckman
spec:
  type: ClusterIP    # 集群外需要访问改为 NodePort / LoadBalancer，或走 Ingress
  selector:
    app: ckman
  ports:
    - name: http
      port: 8808
      targetPort: 8808
```

### Ingress（可选）

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ckman
  namespace: ckman
  annotations:
    # 透传客户端 IP，CKMAN JWT 与 IP 绑定，必须设置
    nginx.ingress.kubernetes.io/configuration-snippet: |
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
spec:
  ingressClassName: nginx
  rules:
    - host: ckman.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: ckman
                port:
                  number: 8808
```

### 一键部署

把上述 YAML 合并到 `ckman.yaml`：

```bash
kubectl apply -f ckman.yaml
kubectl -n ckman get pod,svc,ingress
kubectl -n ckman logs -f deploy/ckman
```

### 升级

```bash
# 改 image tag
kubectl -n ckman set image deployment/ckman ckman=quay.io/housepower/ckman:vX.Y.Z

# 看滚动状态
kubectl -n ckman rollout status deployment/ckman
```

### 监控接入

CKMAN 默认在 `/metrics` 暴露 Prometheus 指标。如果集群里跑 prometheus-operator，加 `ServiceMonitor`：

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ckman
  namespace: ckman
spec:
  selector:
    matchLabels:
      app: ckman
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

::: tip 被管控的 ClickHouse 不在 K8s 内
K8s 里跑的只是 CKMAN 自身。**被管控的 ClickHouse 集群通常还是部署在物理机/虚拟机上**——CKMAN 通过 SSH 登录节点完成 ClickHouse 的安装与运维。如果你希望 ClickHouse 也跑在 K8s 内，目前需要自己用 Operator（例如 [Altinity ClickHouse Operator](https://github.com/Altinity/clickhouse-operator)）部署，再以 `import` 模式纳入 CKMAN。
:::

## 首次登录

| 字段 | 默认值 |
| --- | --- |
| 地址 | `http://<host>:8808` |
| 用户名 | `ckman` |
| 密码 | `Ckman123456!`（首次登录建议立刻修改） |

登录后在「用户管理 → 修改密码」中替换默认密码。

::: tip
旧版本通过 `conf/password` 文件 + `ckmanpassword` 工具管理密码，**新版已移除**，账号统一存放在持久层（local SQLite / MySQL / PostgreSQL / DM8），通过 Web 界面或 API 管理。
:::

## 监控集成（可选）

::: tip 默认不需要
CKMAN 的 Overview 监控面板**直读 ClickHouse 系统表**（`system.metric_log` / `asynchronous_metric_log`），开箱即可显示指标，**无需 Prometheus**。

仅当你需要以下能力时再接入下面这套 Prometheus + node_exporter + ZK metrics：

- 长期保存指标（CK 系统表 TTL 较短）
- 接入既有 Grafana / 告警体系
- 多集群统一聚合

详见[监控指标 > 何时选哪条路](/features/monitoring/metrics#何时选哪条路)。
:::

### ZooKeeper 监控

在每个 zk 节点的 `zoo.cfg` 加入：

```ini
metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider
metricsProvider.httpPort=7000    # 暴露给 prometheus 的端口
admin.enableServer=true
admin.serverPort=8080            # 用于 mntr 等四字命令，需要 3.5.0+
```

依次重启所有 zk 节点。

### ClickHouse 监控

- **CKMAN 部署的集群**：默认监听 `9363` 端口上报 metric，无需额外配置
- **导入的集群**：确保 `config.xml` 含以下片段

```xml
<prometheus>
  <endpoint>/metrics</endpoint>
  <port>9363</port>
  <metrics>true</metrics>
  <events>true</events>
  <asynchronous_metrics>true</asynchronous_metrics>
  <status_info>true</status_info>
</prometheus>
```

### Node Exporter

`node_exporter` 是节点机操作系统指标采集器，默认 `9100` 端口，需要安装到每台 ClickHouse 节点上。

### Prometheus 配置

```yaml
scrape_configs:
  - job_name: node_exporter
    scrape_interval: 10s
    static_configs:
      - targets: ['192.168.0.1:9100', '192.168.0.2:9100']

  - job_name: clickhouse
    scrape_interval: 10s
    static_configs:
      - targets: ['192.168.0.1:9363', '192.168.0.2:9363']

  - job_name: zookeeper
    scrape_interval: 10s
    static_configs:
      - targets: ['192.168.0.1:7000', '192.168.0.2:7000', '192.168.0.3:7000']
```

::: tip 自动发现
从 v2.3.5 起 CKMAN 支持 Prometheus HTTP service discovery，无需手工维护 targets 列表。详见[监控指标 > 自动发现](/features/monitoring/metrics)。
:::

## 从源码编译

::: info 适用人群
仅当你需要二次开发或临时构建特定 commit 时才需要。日常部署使用上述发行包即可。
:::

依赖：

- Go ≥ 1.17（推荐 1.24）
- `nfpm`（构建 rpm/deb 时需要）

  ```bash
  wget -q https://github.com/goreleaser/nfpm/releases/download/v2.15.1/nfpm_2.15.1_Linux_x86_64.tar.gz
  tar -xzvf nfpm_2.15.1_Linux_x86_64.tar.gz
  sudo cp nfpm /usr/local/bin
  ```

- `yarn`（构建前端时需要，CentOS 7 安装示例）

  ```bash
  curl --silent --location https://dl.yarnpkg.com/rpm/yarn.repo | sudo tee /etc/yum.repos.d/yarn.repo
  sudo rpm --import https://dl.yarnpkg.com/rpm/pubkey.gpg
  sudo yum install yarn
  ```

构建命令：

```bash
# tar.gz
make package VERSION=x.x.x

# rpm
make rpm VERSION=x.x.x

# 仅前端
cd frontend && yarn && cd ..
make frontend

# Docker 内编译（无需自备依赖）
make docker-build VERSION=x.x.x
```

`VERSION` 未指定时自动取 `git describe --tags --dirty`。
