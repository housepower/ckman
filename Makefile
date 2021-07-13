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
ARCH=$(shell uname -m)
TARNAME=${PKGDIR}-${VERSION}-${DATE}.${OS}.$(ARCH).tar.gz
TAG?=$(shell date +%y%m%d)
LDFLAGS=-ldflags "-X main.BuildTimeStamp=${TIME} -X main.GitCommitHash=${REVISION} -X main.Version=${VERSION}"
PUB_KEY=$(shell cat resources/eoi_public_key.pub 2>/dev/null)
export GOPROXY=https://goproxy.cn,direct

.PHONY: frontend
frontend:
	rm -rf static/dist
	make -C frontend build
	cp -r frontend/dist static

.PHONY: backend
backend:
	@rm -rf ${PKGFULLDIR}
	go build ${LDFLAGS}
	go build ${LDFLAGS} -o ckmanpasswd password/password.go
	go build ${LDFLAGS} -o schemer cmd/schemer/schemer.go
	go build ${LDFLAGS} -o rebalancer cmd/rebalancer/rebalancer.go
	go build ${LDFLAGS} -o exporter cmd/exporter/exporter.go
	go build ${LDFLAGS} -o purger cmd/purger/purger.go

.PHONY: pre
pre:
	go mod tidy
	go get github.com/markbates/pkger/cmd/pkger
	go get github.com/swaggo/swag/cmd/swag

.PHONY: build
build:pre
	@test -d static/dist || (git submodule update --init --recursive && make frontend)
	pkger
	swag init
	go build ${LDFLAGS}
	go build ${LDFLAGS} -o ckmanpasswd password/password.go
	go build ${LDFLAGS} -o schemer cmd/schemer/schemer.go
	go build ${LDFLAGS} -o rebalancer cmd/rebalancer/rebalancer.go
	go build ${LDFLAGS} -o exporter cmd/exporter/exporter.go
	go build ${LDFLAGS} -o purger cmd/purger/purger.go

.PHONY: package
package:build
	@rm -rf ${PKGFULLDIR_TMP}
	@mkdir -p ${PKGFULLDIR_TMP}/bin ${PKGFULLDIR_TMP}/conf ${PKGFULLDIR_TMP}/run ${PKGFULLDIR_TMP}/logs ${PKGFULLDIR_TMP}/package ${PKGFULLDIR_TMP}/template
	@mv ${SHDIR}/ckman ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/ckmanpasswd ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/rebalancer ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/schemer ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/exporter ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/purger ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/start ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/stop ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/config.xml ${PKGFULLDIR_TMP}/template
	@cp ${SHDIR}/resources/users.xml ${PKGFULLDIR_TMP}/template
	@cp ${SHDIR}/resources/ckman.yaml ${PKGFULLDIR_TMP}/conf/ckman.yaml
	@cp ${SHDIR}/resources/password ${PKGFULLDIR_TMP}/conf/password
	@cp ${SHDIR}/README.md ${PKGFULLDIR_TMP}
	@test ! -f resources/eoi_public_key.pub || (sed -i "s|#public_key:|${PUB_KEY}|" ${PKGFULLDIR_TMP}/conf/ckman.yaml)
	@mv ${PKGFULLDIR_TMP} ${PKGFULLDIR}
	@echo "create ${TARNAME} from ${PKGDIR}"
	@tar -czf ${TARNAME} ${PKGDIR}
	@rm pkged.go
	@rm -rf ${PKGFULLDIR}

.PHONY: docker-build
docker-build:
	rm -rf ${PKGDIR}-*.tar.gz
	docker run --rm -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct eoitek/ckman-build:go-1.16 make package VERSION=${VERSION}

.PHONY: docker-sh
docker-sh:
	docker run --rm  -it -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct eoitek/ckman-build:go-1.16 bash

.PHONY: rpm
rpm:build
	@sed "s/trunk/${VERSION}/g" nfpm.yaml > nfpm_${VERSION}.yaml
	nfpm -f nfpm_${VERSION}.yaml pkg --packager rpm --target .
	@rm nfpm_${VERSION}.yaml

.PHONY: deb
deb:build
	@sed "s/trunk/${VERSION}/g" nfpm.yaml > nfpm_${VERSION}.yaml
	nfpm -f nfpm_${VERSION}.yaml pkg --packager deb --target .
	@rm nfpm_${VERSION}.yaml

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

.PHONY: release
release:
	make docker-image VERSION=${VERSION}
	make rpm VERSION=${VERSION}
	make package VERSION=${VERSION}
	docker push quay.io/housepower/ckman:${VERSION}
	docker push quay.io/housepower/ckman:latest

.PHONY: lint
lint:
	golangci-lint run -D errcheck,govet
