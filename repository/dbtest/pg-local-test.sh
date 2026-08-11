#!/usr/bin/env bash
# 保留这个名字是为了兼容旧的调用习惯，实际逻辑已并入 local-test.sh。
# 新用法见 `make dbtest-pg` / `./repository/dbtest/local-test.sh postgres`。
set -euo pipefail
exec "$(dirname "$0")/local-test.sh" postgres
