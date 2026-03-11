# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CKMAN is a ClickHouse cluster management and monitoring tool. It provides a web UI for deploying, upgrading, managing, and monitoring ClickHouse clusters.

**Tech Stack:**
- **Backend**: Go 1.24 with Gin web framework
- **Frontend**: Vue.js (TypeScript) in `frontend/` directory
- **Database**: ClickHouse for managed clusters, persistent layer supports local, MySQL, PostgreSQL, DM8
- **Service Discovery**: Nacos (optional) for multi-instance deployment
- **Coordination**: ZooKeeper for ClickHouse distributed operations
- **Auth**: JWT tokens + unified portal user tokens

## Build Commands

```bash
# Full build (frontend + backend + swagger docs)
make build

# Build only backend
make backend

# Build with debug symbols
make debug

# Build frontend
make frontend

# Run all tests
make test

# Run tests with coverage
make coverage

# Lint code
make lint

# Create packages (tar.gz, rpm, deb)
make package
make rpm
make deb

# Run single test package
go test ./<package> -v

# Generate swagger docs
swag init
```

## Architecture

### Layer Structure
```
server/         # HTTP server setup, middleware, and routing
controller/     # HTTP request handlers
service/        # Business logic (clickhouse, zookeeper, cron, runner, prometheus)
repository/     # Persistent storage abstraction with multiple backends
model/          # Data models and structs
config/         # Configuration management (HJSON/YAML support)
common/         # Shared utilities (logging, crypto, pool, workerpool)
router/         # API route definitions (v1 and v2)
ckconfig/       # ClickHouse config generation logic
```

### API Versioning
- **v1**: Legacy API endpoints (`/api/v1/`)
- **v2**: Current API endpoints (`/api/v2/`) - primary focus for new features

### Persistent Layer Pattern
The repository layer uses a factory pattern for pluggable backends:
- `repository/persistent.go` defines interfaces for cluster, logic cluster, query history, task, and backup operations
- `repository/{local,mysql,postgres,dm8}/` provide implementations
- Configuration selects backend via `persistent_policy` setting

### Authentication Flow
1. Login endpoint issues JWT token with username, client IP, and expiration
2. Middleware checks token validity, expiration, and IP binding
3. Unified portal tokens (RSA-encrypted) can bypass JWT
4. Token cache stored in `controller.TokenCache` (in-memory cache)

### Task System
Async operations use a task queue:
- Tasks created via `controller.TaskController`
- Stored in persistent layer (`PersistentTaskService`)
- Runner service (`service/runner`) processes pending tasks
- Tasks tracked by ID with status updates

### Multi-Instance Deployment
When Nacos is enabled:
- Each instance registers with Nacos heartbeat
- Only master instance executes scheduled tasks (cron jobs)
- `config.IsMasterNode()` determines if current instance should run tasks
- Cluster node list stored in `config.ClusterNodes`

## Configuration

Main config file: `conf/ckman.hjson` (HJSON format, YAML also supported)

Key config sections:
- `server`: HTTP server, session timeout, auth settings, metrics path
- `clickhouse`: Connection pool settings for CKMAN's ClickHouse connections
- `log`: Logging level, rotation settings
- `persistent_config`: Backend-specific storage config
- `nacos`: Service discovery config
- `cron`: Scheduled job settings

Environment variables:
- `NACOS_HOST`: Comma-separated host:port list for Nacos
- `HOST_IP`: Override server IP address

## Frontend Development

Frontend is in `frontend/` directory:
```bash
cd frontend
make install    # Install dependencies
make dev        # Development server with hot reload
make build      # Production build to static/dist/
make lint       # Lint code
```

The built frontend is embedded in the Go binary using `//go:embed static/dist`.

## Key Patterns

### Response Wrapping
APIs use `router.WrapMsg()` or `router.WrapMsg2()` to standardize responses:
- Code/Msg/Data structure
- Automatic ClickHouse exception decoding
- Error logging on failure

### ClickHouse Operations
Most cluster operations go through `controller.ClickHouseController`:
- Cluster CRUD, start/stop, upgrade, destroy, rebalance
- Table management (create, alter, delete, truncate, TTL, readonly)
- Query execution and history tracking
- Partition management, data archive/purge
- Node management (add, delete, start, stop)
- Distributed DDL queue monitoring

### Connection Pooling
- `common.ConnectPool`: ClickHouse connection pool by cluster
- `common.Pool`: Worker pool for concurrent operations
- Connection management in `common/workerpool.go`

### Config Generation
ClickHouse config files generated in `ckconfig/`:
- `metrika.go`: ZooKeeper and distributed table config
- `users.go`: User management config
- `custom.go`: Custom settings
- `keeper.go`: ClickHouse Keeper config

## Testing

Test files use standard Go testing:
- Unit tests: `*_test.go` files
- Run all tests: `go test ./... -v`
- Coverage: `go test ./... -coverprofile=coverage.txt`

## Development Notes

- Frontend changes require `make frontend` to rebuild into `static/dist/`
- API changes require `swag init` to regenerate swagger docs
- Backend uses embedded filesystem - no static file serving needed
- Configuration passwords are encrypted using `common.AesEncryptECB()`
- JWT tokens bound to client IP for security
- ZooKeeper operations cached in `zookeeper.ZkServiceCache` (1hr TTL)
