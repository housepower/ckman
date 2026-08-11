#!/usr/bin/env bash
# 本地真库回归：自动拉起 MySQL / PostgreSQL 容器 -> 跑 dbtest -> 删容器。
#
#   ./repository/dbtest/local-test.sh              # 跑 mysql + postgres
#   ./repository/dbtest/local-test.sh mysql        # 只跑 mysql
#   ./repository/dbtest/local-test.sh postgres     # 只跑 postgres
#
# 后端建表靠各 Persistent 的 Init(AutoMigrate/ensureSchema)，所以指向空库即可，
# 不需要预先导入任何 SQL。
#
# 达梦没有公开镜像，起不了容器，要测达梦得自己指一台实例：
#
#   DBTEST_DM_HOST=1.2.3.4 DBTEST_DM_USER=SYSDBA DBTEST_DM_PASS=xxx \
#   go test -tags dbintegration -v ./repository/dbtest/ -run TestBackends/dm8
set -euo pipefail

WHICH=${1:-all}

MYSQL_NAME=ckman-dbtest-mysql
MYSQL_PORT=${DBTEST_MYSQL_PORT:-3307}
MYSQL_PASS=${DBTEST_MYSQL_PASS:-Test@1234}
MYSQL_DB=${DBTEST_MYSQL_DB:-ckman_db}
MYSQL_IMAGE=${DBTEST_MYSQL_IMAGE:-mysql:8.0.31}

PG_NAME=ckman-dbtest-pg
PG_PORT=${DBTEST_PG_PORT:-5433}
PG_PASS=${DBTEST_PG_PASS:-Test@1234}
PG_DB=${DBTEST_PG_DB:-ckman_db}
PG_IMAGE=${DBTEST_PG_IMAGE:-postgres:16}

cleanup() { docker rm -f "$MYSQL_NAME" "$PG_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

# 仓库根目录(脚本位于 repository/dbtest/ 下)
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

RUN="TestBackends"

if [[ "$WHICH" == "all" || "$WHICH" == "mysql" ]]; then
    echo ">> 启动 $MYSQL_IMAGE (容器 $MYSQL_NAME, 端口 $MYSQL_PORT)"
    docker run -d --name "$MYSQL_NAME" \
        -e MYSQL_ROOT_PASSWORD="$MYSQL_PASS" -e MYSQL_DATABASE="$MYSQL_DB" \
        -p "$MYSQL_PORT":3306 "$MYSQL_IMAGE" >/dev/null
    echo -n ">> 等待 MySQL 就绪"
    for _ in $(seq 1 60); do
        if docker exec "$MYSQL_NAME" mysqladmin ping -uroot -p"$MYSQL_PASS" 2>/dev/null | grep -q alive; then
            echo " ok"; break
        fi
        echo -n .; sleep 1
    done
    export DBTEST_MYSQL_HOST=127.0.0.1 DBTEST_MYSQL_PORT="$MYSQL_PORT" \
           DBTEST_MYSQL_USER=root DBTEST_MYSQL_PASS="$MYSQL_PASS" DBTEST_MYSQL_DB="$MYSQL_DB"
    [[ "$WHICH" == "mysql" ]] && RUN="TestBackends/mysql"
fi

if [[ "$WHICH" == "all" || "$WHICH" == "postgres" ]]; then
    echo ">> 启动 $PG_IMAGE (容器 $PG_NAME, 端口 $PG_PORT)"
    docker run -d --name "$PG_NAME" \
        -e POSTGRES_PASSWORD="$PG_PASS" -e POSTGRES_DB="$PG_DB" \
        -p "$PG_PORT":5432 "$PG_IMAGE" >/dev/null
    echo -n ">> 等待 PostgreSQL 就绪"
    for _ in $(seq 1 60); do
        if docker exec "$PG_NAME" pg_isready -U postgres -d "$PG_DB" >/dev/null 2>&1; then
            echo " ok"; break
        fi
        echo -n .; sleep 1
    done
    export DBTEST_PG_HOST=127.0.0.1 DBTEST_PG_PORT="$PG_PORT" \
           DBTEST_PG_USER=postgres DBTEST_PG_PASS="$PG_PASS" DBTEST_PG_DB="$PG_DB"
    [[ "$WHICH" == "postgres" ]] && RUN="TestBackends/postgres"
fi

echo ">> 跑 dbtest ($RUN)"
go test -tags dbintegration -count=1 -v ./repository/dbtest/ -run "$RUN"
