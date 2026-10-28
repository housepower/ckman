# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CKMAN is a ClickHouse cluster management and monitoring tool. It provides a web UI for deploying, upgrading, managing, and monitoring ClickHouse clusters. Backend is Go (Gin) with an embedded Vue.js frontend (`frontend/` submodule).

Build/test/lint entry points are the `Makefile` targets (`make build`, `make test`, …); frontend has its own `frontend/Makefile`.

## API Versioning

- **v1**: Legacy API endpoints (`/api/v1/`)
- **v2**: Current API endpoints (`/api/v2/`) — primary focus for new features

## Configuration

Main config file: `resources/ckman.hjson` (HJSON format, YAML also supported). Notable environment variables: `NACOS_HOST`, `HOST_IP`.

## IMPORTANT: Frontend submodule workflow

`frontend/` is a git submodule (clone of `ckman-fe.git`). **Never edit code or commit inside `frontend/`.** Its HEAD is frequently detached, commits there don't land on any branch and are unreachable to remote/other clones.

Correct workflow:

1. All frontend changes go in the sibling working copy: `../ckman-fe/` (i.e. `/data/root/go/src/github.com/housepower/ckman-fe/`)
2. In `ckman-fe`, commit to the active branch (typically `main`)
3. After pushing, in the parent repo: `cd frontend && git fetch && git checkout <new-sha> && cd .. && git add frontend && git commit` to update the submodule pointer

Before any frontend work, verify `cd ../ckman-fe && git status && git branch -vv` — confirm clean tree and which branch is active. If unsure, ask the user.

`make build` to verify frontend changes is run in `ckman-fe/`, not `ckman/frontend`.

## Multi-Instance Deployment

When Nacos is enabled, each instance registers with a heartbeat and only the master instance (`config.IsMasterNode()`) executes scheduled tasks (cron jobs). Cluster node list stored in `config.ClusterNodes`.

## Testing

- Unit tests: `go test ./... -v`; coverage: `make coverage`
- **Any change touching the persistent layer** (`repository/{mysql,postgres,dm8}/`, the models, time handling, schema/migration, repository interfaces) **MUST be verified against real databases** — mocks cannot catch driver/DB-boundary bugs. Full policy and commands: `repository/CLAUDE.md`.

## Development Notes

- API changes require `swag init` to regenerate swagger docs
- Configuration passwords are encrypted using `common.AesEncryptECB()`
- JWT tokens bound to client IP for security
- ZooKeeper operations cached in `zookeeper.ZkServiceCache` (1hr TTL)
