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

.PHONY: frontend
frontend:
	rm -rf static/dist/*
	make -C frontend build
	cp -r frontend/dist static/
	cp -r static/docs static/dist/

.PHONY: backend
backend:
	@rm -rf ${PKGFULLDIR}
	go build ${LDFLAGS}
	go build ${LDFLAGS} -o cmd/ckmanctl/ckmanctl cmd/ckmanctl/ckmanctl.go

.PHONY: debug
debug:
	@rm -rf ${PKGFULLDIR}
	go build ${GCFLAGS} ${LDFLAGS}
	go build ${LDFLAGS} -o cmd/ckmanctl/ckmanctl cmd/ckmanctl/ckmanctl.go

.PHONY: pre
pre:
	go mod tidy
	go mod vendor
	go install github.com/swaggo/swag/cmd/swag@v1.7.1
	go install github.com/hjson/hjson-go/hjson-cli@latest
	go install github.com/mbrukman/yaml2json/cmd/{yaml2json,json2yaml}@latest

.PHONY: test
test:
	go test ./... -v

.PHONY: coverage
coverage:
	go test ./... -coverprofile=coverage.txt -covermode count
	go tool cover -func coverage.txt
	gocover-cobertura < coverage.txt > coverage.xml	

.PHONY: build
build:pre frontend
	swag init
	make backend VERSION=${VERSION}

.PHONY:check
check:pre
	hjson-cli -j resources/ckman.hjson |jq
	hjson-cli -j resources/migrate.hjson |jq

.PHONY: package
package:build
	@rm -rf ${PKGFULLDIR_TMP}
	@mkdir -p ${PKGFULLDIR_TMP}/bin ${PKGFULLDIR_TMP}/conf ${PKGFULLDIR_TMP}/run ${PKGFULLDIR_TMP}/logs ${PKGFULLDIR_TMP}/package ${PKGFULLDIR_TMP}/dbscript
	@mv ${SHDIR}/ckman ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/cmd/ckmanctl/ckmanctl ${PKGFULLDIR_TMP}/bin
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
docker-build:
	rm -rf ${PKGDIR}-*.tar.gz
	docker run --rm -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct eoitek/ckman-build:go-1.17 make package VERSION=${VERSION}

.PHONY: docker-sh
docker-sh:
	docker run --rm  -it -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct eoitek/ckman-build:go-1.17 bash

.PHONY: rpm
rpm:build
	@cp ${SHDIR}/resources/yaml2json.${GOARCH}  ${SHDIR}/resources/yaml2json
	VERSION=${VERSION} nfpm -f nfpm.yaml pkg --packager rpm --target .
	@rm ${SHDIR}/resources/yaml2json

.PHONY: deb
deb:build
	@cp ${SHDIR}/resources/yaml2json.${GOARCH}  ${SHDIR}/resources/yaml2json
	VERSION=${VERSION} nfpm -f nfpm.yaml pkg --packager deb --target .
	@rm ${SHDIR}/resources/yaml2json

.PHONY: test-ci
test-ci:package
	@rm -rf /tmp/ckman
	@tar -xzf ${TARNAME} -C /tmp
	@cp -r ./tests /tmp/ckman
	@docker-compose stop
	@docker-compose up -d
	@bash ./docker_env.sh
	@bash ./go.test.sh
	@docker-compose down -v

.PHONY: docker-image
docker-image:build
	docker build -t ckman:${VERSION} .
	docker tag ckman:${VERSION} quay.io/housepower/ckman:${VERSION}
	docker tag ckman:${VERSION} quay.io/housepower/ckman:latest
	docker rmi ckman:${VERSION}


.PHONY: internel-release
internel-release:
	#git tag ${VERSION}
	make rpm
	make package VERSION=${VERSION}
	make deb
	make rpm GOARCH=arm64
	make package VERSION=${VERSION} GOARCH=arm64
	make deb GOARCH=arm64

.PHONY: release
release: internel-release
	make docker-image VERSION=${VERSION}
	docker push quay.io/housepower/ckman:${VERSION}
	docker push quay.io/housepower/ckman:latest

.PHONY: lint
lint:
	golangci-lint run -D errcheck,govet,gosimple
