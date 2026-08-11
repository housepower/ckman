GOPACKAGES=$(shell go list -mod vendor ./... | grep -v /vendor/)
SHDIR=$(shell pwd)
PKGDIR=ckman
PKGDIR_TMP=ckman_
PKGFULLDIR=${SHDIR}/${PKGDIR}
PKGFULLDIR_TMP=${SHDIR}/${PKGDIR_TMP}
VERSION=$(shell git describe --tags --dirty)
REVISION=$(shell git rev-parse HEAD)
DATE=$(shell date +%y%m%d)
TIME=$(shell date --iso-8601=seconds 2>/dev/null)
OS=$(shell uname)
OSLOWER=$(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH?=$(shell go env GOARCH)
TARNAME=${PKGDIR}-${VERSION}-${DATE}.${OS}.$(GOARCH).tar.gz
TAG?=$(shell date +%y%m%d)
LDFLAGS=-ldflags "-X main.BuildTimeStamp=${TIME} -X main.GitCommitHash=${REVISION} -X main.Version=${VERSION}"
GCFLAGS=-gcflags "all=-N -l"
PUB_KEY=$(shell cat resources/eoi_public_key.pub 2>/dev/null)
export GOPROXY=https://goproxy.cn,direct

# frontend：按 submodule HEAD SHA 做 stamp。SHA 没变就跳过编译，省 30-60s+。
# 想强制重编：make frontend-force（bump submodule 也会自动失效 stamp）。
FRONTEND_SHA := $(shell git -C frontend rev-parse HEAD 2>/dev/null || echo unknown)
FRONTEND_STAMP := static/dist/.frontend.$(FRONTEND_SHA)

.PHONY: help
help: ## 列出所有可用目标
	@echo "用法: make <目标>"
	@echo
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sed 's/:[a-zA-Z0-9_$$(). -]*##/: ##/' \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

.PHONY: frontend
frontend: $(FRONTEND_STAMP) ## 编译前端(SHA 没变则跳过)

$(FRONTEND_STAMP):
	@echo "==> building frontend ($(FRONTEND_SHA))"
	@rm -rf static/dist/.frontend.* static/dist/index.html static/dist/assets static/dist/static static/dist/favicon.ico
	@make -C frontend build
	@cp -r frontend/dist/. static/dist/
	@touch $@

.PHONY: frontend-force
frontend-force: ## 强制重编前端
	@rm -f static/dist/.frontend.*
	@$(MAKE) frontend

# docs：按 website/ 源码 mtime 做 stamp，不改文档就跳过 VitePress 编译。
# 强制重编：make docs-force。
DOCS_SRC := $(shell find website -type f \
	\( -name '*.md' -o -name '*.mts' -o -name '*.ts' -o -name '*.vue' \
	   -o -name 'package.json' -o -name 'package-lock.json' \) \
	-not -path '*/node_modules/*' \
	-not -path '*/.vitepress/dist/*' \
	-not -path '*/.vitepress/cache/*' 2>/dev/null)
DOCS_STAMP := static/dist/docs/.stamp

.PHONY: docs
docs: $(DOCS_STAMP) ## 生成文档(内容没变则跳过)

$(DOCS_STAMP): $(DOCS_SRC)
	@echo "==> building VitePress docs"
	@cd website && (test -d node_modules || npm install --no-audit --no-fund --silent)
	@cd website && npm run docs:build --silent
	@mkdir -p static/dist/docs
	@cp -r website/.vitepress/dist/. static/dist/docs/
	@touch $@
	@echo "==> docs embedded at static/dist/docs/ (访问 /docs/)"

.PHONY: docs-force
docs-force: ## 强制重新生成文档
	@rm -f $(DOCS_STAMP)
	@$(MAKE) docs

.PHONY: backend
backend: ## 只编译后端二进制(ckman/ckmanctl/watchdog)
	@rm -rf ${PKGFULLDIR}
	CGO_ENABLED=0 go build ${LDFLAGS}
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/ckmanctl/ckmanctl cmd/ckmanctl/ckmanctl.go
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/ckman-watchdog/ckman-watchdog cmd/ckman-watchdog/ckman-watchdog.go

.PHONY: debug
debug: ## 编译带调试符号的后端二进制
	@rm -rf ${PKGFULLDIR}
	CGO_ENABLED=0 go build ${GCFLAGS} ${LDFLAGS}
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/ckmanctl/ckmanctl cmd/ckmanctl/ckmanctl.go
	CGO_ENABLED=0 go build ${LDFLAGS} -o cmd/ckman-watchdog/ckman-watchdog cmd/ckman-watchdog/ckman-watchdog.go

.PHONY: pre
pre: ## go mod tidy/vendor 并安装 swag 等构建工具
	go mod tidy
	go mod vendor
	go install github.com/swaggo/swag/cmd/swag@v1.7.1
	go install github.com/hjson/hjson-go/hjson-cli@latest
	go install github.com/mbrukman/yaml2json/cmd/{yaml2json,json2yaml}@latest

.PHONY: test
test: ## 单元测试(不含真库集成测试，那些带 dbintegration tag)
	go test ./... -v

.PHONY: coverage
coverage: ## 单元测试覆盖率报告
	go test ./... -coverprofile=coverage.txt -covermode count
	go tool cover -func coverage.txt
	gocover-cobertura < coverage.txt > coverage.xml

# ── 持久化后端真库集成测试(repository/dbtest，详见该目录 README.md) ────────────
# 这些用例带 dbintegration build tag，不参与 make test / make build。
# 建表靠各 Persistent 的 Init(AutoMigrate/ensureSchema)，指向空的测试库即可。

DBTEST_PKG = ./repository/dbtest/
DBTEST_RUN = go test -tags dbintegration -count=1

.PHONY: dbtest
dbtest: ## 真库回归：自动拉起 MySQL+PG 容器跑完删容器(要 docker，无需预备库)
	./repository/dbtest/local-test.sh

.PHONY: dbtest-mysql
dbtest-mysql: ## 同上，只跑 MySQL
	./repository/dbtest/local-test.sh mysql

.PHONY: dbtest-pg
dbtest-pg: ## 同上，只跑 PostgreSQL
	./repository/dbtest/local-test.sh postgres

.PHONY: dbtest-dm
dbtest-dm: ## 跑达梦(没有公开镜像，需先 export DBTEST_DM_HOST/USER/PASS)
	@test -n "$$DBTEST_DM_HOST" || { echo "需要先设置 DBTEST_DM_HOST / DBTEST_DM_USER / DBTEST_DM_PASS，见 repository/dbtest/README.md"; exit 1; }
	$(DBTEST_RUN) -v $(DBTEST_PKG) -run TestBackends/dm8

.PHONY: dbtest-env
dbtest-env: ## 按已 export 的 DBTEST_* 跑所有配了的后端(没配的自动 Skip)
	$(DBTEST_RUN) -v $(DBTEST_PKG) -run TestBackends

.PHONY: build
build:pre frontend docs ## 完整构建(前端+文档+后端)
	swag init
	make backend VERSION=${VERSION}

.PHONY:check
check:pre ## 校验 hjson 配置文件能否正确解析
	hjson-cli -j resources/ckman.hjson |jq
	hjson-cli -j resources/migrate.hjson |jq

.PHONY: package
package:build ## 打出可发布的 tar 包
	@rm -rf ${PKGFULLDIR_TMP}
	@mkdir -p ${PKGFULLDIR_TMP}/bin ${PKGFULLDIR_TMP}/conf ${PKGFULLDIR_TMP}/run ${PKGFULLDIR_TMP}/logs ${PKGFULLDIR_TMP}/package ${PKGFULLDIR_TMP}/dbscript
	@mv ${SHDIR}/ckman ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/cmd/ckmanctl/ckmanctl ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/cmd/ckman-watchdog/ckman-watchdog ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/start ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/stop ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/yaml2json.${GOARCH} ${PKGFULLDIR_TMP}/bin/yaml2json
	@cp ${SHDIR}/resources/ckman.hjson ${PKGFULLDIR_TMP}/conf/ckman.hjson
	@cp ${SHDIR}/resources/migrate.hjson ${PKGFULLDIR_TMP}/conf/migrate.hjson
	@cp ${SHDIR}/resources/password ${PKGFULLDIR_TMP}/conf/password
	@cp ${SHDIR}/resources/server.key ${PKGFULLDIR_TMP}/conf/server.key
	@cp ${SHDIR}/resources/server.crt ${PKGFULLDIR_TMP}/conf/server.crt
	@cp ${SHDIR}/resources/postgres.sql ${PKGFULLDIR_TMP}/dbscript/postgres.sql
	@cp ${SHDIR}/README.md ${PKGFULLDIR_TMP}
	@mv ${PKGFULLDIR_TMP} ${PKGFULLDIR}
	@echo "create ${TARNAME} from ${PKGDIR}"
	@tar -czf ${TARNAME} ${PKGDIR}
	@rm -rf ${PKGFULLDIR}

.PHONY: docker-build
docker-build: ## 在构建镜像里打包
	rm -rf ${PKGDIR}-*.tar.gz
	docker run --rm -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct eoitek/ckman-build:go-1.17 make package VERSION=${VERSION}

.PHONY: docker-sh
docker-sh: ## 进构建镜像的 shell
	docker run --rm  -it -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct eoitek/ckman-build:go-1.17 bash

.PHONY: rpm
rpm:build ## 打 rpm 包
	@cp ${SHDIR}/resources/yaml2json.${GOARCH}  ${SHDIR}/resources/yaml2json
	VERSION=${VERSION} nfpm -f nfpm.yaml pkg --packager rpm --target .
	@rm ${SHDIR}/resources/yaml2json

.PHONY: deb
deb:build ## 打 deb 包
	@cp ${SHDIR}/resources/yaml2json.${GOARCH}  ${SHDIR}/resources/yaml2json
	VERSION=${VERSION} nfpm -f nfpm.yaml pkg --packager deb --target .
	@rm ${SHDIR}/resources/yaml2json

.PHONY: test-ci
test-ci:package ## 端到端测试：起 ClickHouse 集群跑 go.test.sh
	@rm -rf /tmp/ckman
	@tar -xzf ${TARNAME} -C /tmp
	@cp -r ./tests /tmp/ckman
	@docker-compose stop
	@docker-compose up -d
	@bash ./docker_env.sh
	@bash ./go.test.sh
	@docker-compose down -v

.PHONY: docker-image
docker-image:build ## 构建 docker 镜像
	docker build -t ckman:${VERSION} .
	docker tag ckman:${VERSION} hub.eoitek.net/aimeter/ckman:${VERSION}
	docker rmi ckman:${VERSION}


.PHONY: internel-release
internel-release: ## 打 amd64+arm64 的 rpm/deb/tar
	#git tag ${VERSION}
	make rpm
	make package VERSION=${VERSION}
	make deb
	make rpm GOARCH=arm64
	make package VERSION=${VERSION} GOARCH=arm64
	make deb GOARCH=arm64

.PHONY: release
release: internel-release ## 内部发布 + 构建推送镜像
	make docker-image VERSION=${VERSION}
	docker push hub.eoitek.net/housepower/ckman:${VERSION}
	docker push hub.eoitek.net/housepower/ckman:latest

.PHONY: lint
lint: ## golangci-lint 静态检查
	golangci-lint run -D errcheck,govet,gosimple
