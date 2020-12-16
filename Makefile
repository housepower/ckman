GOPACKAGES=$(shell go list -mod vendor ./... | grep -v /vendor/)
SHDIR=$(shell pwd)
PKGDIR=ckman
PKGDIR_TMP=ckman_
PKGFULLDIR=${SHDIR}/${PKGDIR}
PKGFULLDIR_TMP=${SHDIR}/${PKGDIR_TMP}
REVISION=$(shell git log --oneline | head -n1 | cut -f1 -d" ")
DATE=$(shell date +%y%m%d)
TIME=$(shell date +%y%m%dT%H:%M:%S)
OS=$(shell uname)
OSLOWER=$(shell uname | tr '[:upper:]' '[:lower:]')
ARCH=$(shell uname -m)
TARNAME=${PKGDIR}-${VERSION}-${DATE}-${REVISION}.${OS}.$(ARCH).tar.gz
VERSION?=trunk
TAG?=$(shell date +%y%m%d)

.PHONY: backend
backend:
	@rm -rf ${PKGFULLDIR}
	go build -ldflags "-X main.BuildTimeStamp=${TIME} -X main.GitCommitHash=${REVISION} -X main.Version=ckman-${VERSION}"
	go build -o ckmanpasswd password/password.go
	go build -o schemer cmd/schemer/schemer.go
	go build -o rebalancer cmd/rebalancer/rebalancer.go

.PHONY: build
build:
	@rm -rf ${PKGFULLDIR}
	make -C frontend build
	pkger
	go build -ldflags "-X main.BuildTimeStamp=${TIME} -X main.GitCommitHash=${REVISION} -X main.Version=ckman-${VERSION}"
	go build -o ckmanpasswd password/password.go
	go build -o schemer cmd/schemer/schemer.go
	go build -o rebalancer cmd/rebalancer/rebalancer.go

.PHONY: package
package: build
	@rm -rf ${PKGFULLDIR_TMP}
	@mkdir -p ${PKGFULLDIR_TMP}/bin ${PKGFULLDIR_TMP}/conf ${PKGFULLDIR_TMP}/run ${PKGFULLDIR_TMP}/logs ${PKGFULLDIR_TMP}/package ${PKGFULLDIR_TMP}/template
	@mv ${SHDIR}/ckman ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/ckmanpasswd ${PKGFULLDIR_TMP}/bin
	@mv ${SHDIR}/schemer ${PKGFULLDIR_TMP}/bin
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

.PHONY: docker-build
docker-build:
	rm -rf ${PKGDIR}-*.tar.gz
	docker run --rm -v "$$PWD":/var/ckman -w /var/ckman -e GO111MODULE=on -e GOPROXY=https://goproxy.cn,direct amd64/golang:1.15.3 make package VERSION=${VERSION}

.PHONY: rpm
rpm: build
	nfpm pkg --packager rpm --target .

.PHONY: deb
deb: build
	nfpm pkg --packager deb --target .
