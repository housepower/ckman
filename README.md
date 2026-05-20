ENGLISH | [简体中文](./README_ZH.md)

# CKMAN — ClickHouse Cluster Manager

[![License](https://img.shields.io/github/license/housepower/ckman)](./LICENSE)
[![Release](https://img.shields.io/github/v/release/housepower/ckman)](https://github.com/housepower/ckman/releases)
[![Website](https://img.shields.io/badge/website-housepower.github.io%2Fckman-A88500)](https://housepower.github.io/ckman/)
[![Stars](https://img.shields.io/github/stars/housepower/ckman?style=social)](https://github.com/housepower/ckman/stargazers)

> Official site: **<https://housepower.github.io/ckman/>**

**CKMAN** is an enterprise-grade web console for managing and monitoring **ClickHouse** clusters. Deploy, upgrade, scale, monitor, back up and govern your ClickHouse fleet from a single UI — no more per-node SSH.

<p align="center">
  <img src="website/public/img/guide/architecture.png" alt="CKMAN architecture" width="720">
</p>

## Documentation

- **Official site**: <https://housepower.github.io/ckman/>
- **Local**: start ckman and visit `http://<ckman-host>:8808/docs/`
- **Source**: [`website/`](./website)

## Features

- **Cluster lifecycle** — deploy / upgrade (rolling) / destroy / start-stop / node add-delete, all via Web UI or API
- **Built-in monitoring** — query ClickHouse system tables directly; optional Prometheus / Grafana integration
- **Table & data management** — distributed tables, partitions, TTL, materialized views, DML, archive, purge
- **Backup & restore** — scheduled policies, incremental dedupe, local & S3 targets
- **RBAC** — three roles (admin / ordinary / guest) + JWT + client-IP binding + unified portal token
- **HA deployment** — multi-instance + Nacos master election + MySQL / PostgreSQL / DM8 / SQLite backends
- **ckmanctl CLI** — persistent layer migration, ZooKeeper maintenance, schema upgrade utilities
- **Multiple distributions** — rpm / deb / tar.gz / Docker / Kubernetes

## Quick Start

```bash
docker run -itd -p 8808:8808 --restart unless-stopped \
  --name ckman quay.io/housepower/ckman:latest
```

Open `http://localhost:8808` — default credentials are documented in [Quick Start](https://housepower.github.io/ckman/docs/guide/quick-start.html). Other distributions (rpm / deb / tar.gz / Kubernetes) see [Install](https://housepower.github.io/ckman/docs/deploy/install.html).

## Build from Source

```bash
make build VERSION=x.x.x        # full build (frontend + docs + backend)
make package VERSION=x.x.x      # build tar.gz
make rpm VERSION=x.x.x          # build rpm
make deb VERSION=x.x.x          # build deb
```

Requires Go ≥ 1.17, Node ≥ 18, yarn, and (for rpm/deb) [nfpm](https://github.com/goreleaser/nfpm).

## Video Tutorials

- [Bilibili — ClickHouse 可视化管理工具 ckman 使用教程](https://www.bilibili.com/video/BV1gR4y1t75Q/)
- [Toutiao — ClickHouse 可视化管理工具 ckman 使用教程](https://www.ixigua.com/7034858546692882983)

## Architecture & Concepts

See [Architecture overview](https://housepower.github.io/ckman/docs/guide/architecture.html) and [Core concepts](https://housepower.github.io/ckman/docs/guide/concepts.html).

## Contributing

Issues and pull requests are welcome. Please describe your motivation and impact in the PR body. Maintainer: **YenchangChan** (WeChat: `yudinghou`).

## About Us

EOI Technology Co., Ltd. (上海擎创信息技术有限公司) — domestic AIOps solution vendor. CKMAN is developed by the database team and contributed to open source.

<!-- TODO: 公众号二维码图片，可补到 website/public/img/community/qr.jpg 并在此处引用 -->

## License

Apache License 2.0 — see [LICENSE](./LICENSE).
