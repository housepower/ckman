# log

日志输出与切割配置。

## 配置项

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `level` | string | `INFO` | 日志级别 |
| `max_count` | int | `5` | 保留的滚动日志文件个数 |
| `max_size` | int | `10` | 单个日志文件大小上限（MB） |
| `max_age` | int | `10` | 单个日志文件保留天数 |

## 支持的日志级别

由低到高：

`DEBUG` → `INFO` → `WARN` → `ERROR` → `PANIC` → `FATAL`

::: tip 排查问题
出现异常时可临时把 `level` 改成 `DEBUG`，重启 CKMAN 后能看到详细执行链路（含 ClickHouse SQL、SSH 命令等）。问题定位后建议改回 `INFO`，DEBUG 日志量较大。
:::

## 日志位置

| 安装方式 | 默认日志路径 |
| --- | --- |
| tar.gz | `<工作目录>/logs/ckman.log` |
| rpm | `/var/log/ckman/ckman.log` |

启动时也可用 `-l` 指定：

```bash
ckman -l /custom/path/ckman.log
```

## 示例

```hjson
"log": {
  "level": "INFO",
  "max_count": 10,
  "max_size": 50,
  "max_age": 30
}
```

以上配置每个日志文件 50MB，最多保留 10 个，且最多保留 30 天——总计最多 ≈ 500MB 磁盘占用。
