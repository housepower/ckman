GOPACKAGES=$(shell go list -mod vendor ./... | grep -v /vendor/)
SHDIR=$(shell pwd)
PKGDIR=ckman
PKGDIR_TMP=ckman_
PKGFULLDIR=${SHDIR}/${PKGDIR}
PKGFULLDIR_TMP=${SHDIR}/${PKGDIR_TMP}
REVISION=$(shell git log --oneline | head -n1 | cut -f1 -d" ")
DATE=$(shell date +%y%m%d)
TIME=$(shell date --iso-8601=seconds)
OS=$(shell uname)
OSLOWER=$(shell uname | tr '[:upper:]' '[:lower:]')
ARCH=$(shell uname -m)
TARNAME=${PKGDIR}-${VERSION}-${DATE}-${REVISION}.${OS}.$(ARCH).tar.gz
VERSION?=trunk
TAG?=$(shell date +%y%m%d)
LDFLAGS=-ldflags "-X main.BuildTimeStamp=${TIME} -X main.GitCommitHash=${REVISION} -X main.Version=${VERSION}"

.PHONY: backend
backend:
	@rm -rf ${PKGFULLDIR}
	go build ${LDFLAGS}
	go build ${LDFLAGS} -o ckmanpasswd password/password.go
	go build ${LDFLAGS} -o schemer cmd/schemer/schemer.go
	go build ${LDFLAGS} -o rebalancer cmd/rebalancer/rebalancer.go
	go build ${LDFLAGS} -o exporter cmd/exporter/exporter.go
	go build ${LDFLAGS} -o purger cmd/purger/purger.go

.PHONY: build
build:
	@rm -rf ${PKGFULLDIR}
	make -C frontend build
	pkger
	go build ${LDFLAGS}
	go build ${LDFLAGS} -o ckmanpasswd password/password.go
	go build ${LDFLAGS} -o schemer cmd/schemer/schemer.go
	go build ${LDFLAGS} -o rebalancer cmd/rebalancer/rebalancer.go
	go build ${LDFLAGS} -o exporter cmd/exporter/exporter.go
	go build ${LDFLAGS} -o purger cmd/purger/purger.go

.PHONY: package
package: build
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
rpm: build
	@sed "s/trunk/${VERSION}/g" nfpm.yaml > nfpm_${VERSION}.yaml
	nfpm -f nfpm_${VERSION}.yaml pkg --packager rpm --target .
	@rm nfpm_${VERSION}.yaml

.PHONY: deb
deb: build
	@sed "s/trunk/${VERSION}/g" nfpm.yaml > nfpm_${VERSION}.yaml
	nfpm -f nfpm_${VERSION}.yaml pkg --packager deb --target .
	@rm nfpm_${VERSION}.yaml

.PHONY: test-ci
test-ci:docker-build
	@rm -rf /tmp/ckman
	@tar -xzf ${PKGDIR}-*.Linux.x86_64.tar.gz -C /tmp
	@cp -r ./tests /tmp/ckman
	@cp go.test.sh /tmp/ckman/bin
	@docker-compose stop
	@docker-compose up -d
	@docker run --rm -itd --network ckman_default --privileged=true --name ckman -p8808:8808 -w /tmp/ckman -v /tmp/ckman:/tmp/ckman eoitek/ckman-clickhouse:centos-7
	@bash ./docker_env.sh
	@docker exec -it ckman /tmp/ckman/bin/go.test.sh
	@docker stop ckman
	@docker-compose down -v