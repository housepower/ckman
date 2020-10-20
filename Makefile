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


.PHONY: build
build:
	$(CGO_ARGS) go build -ldflags "-X config.BuildTimeStamp=${TIME} -X config.GitCommitHash=${REVISION} -X config.Version=ckman-${VERSION}" -mod vendor

.PHONY: test
test:
	$(foreach var,$(GOPACKAGES),$(CGO_ARGS) go test -v -mod vendor $(var) || exit 1;)
	$(CGO_ARGS) go test -v -mod vendor .

.PHONY: coverage
coverage:
	echo "mode: count" > coverage-all.out
	$(foreach pkg,$(GOPACKAGES),\
		$(CGO_ARGS) go test -coverprofile=coverage.out -covermode=count $(pkg);\
		tail -n +2 coverage.out >> coverage-all.out;)
	$(CGO_ARGS) go tool cover -func coverage-all.out

.PHONY: package
package: build
	@rm -rf ${PKGFULLDIR_TMP}
	@mkdir -p ${PKGFULLDIR_TMP}/bin ${PKGFULLDIR_TMP}/conf ${PKGFULLDIR_TMP}/run ${PKGFULLDIR_TMP}/logs
	@mv ${SHDIR}/ckman ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/start ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/stop ${PKGFULLDIR_TMP}/bin
	@cp ${SHDIR}/resources/ckman.yml ${PKGFULLDIR_TMP}/conf/ckman.yml
	@mv ${PKGFULLDIR_TMP} ${PKGFULLDIR}
	@echo "create ${TARNAME} from ${PKGDIR}"
	@tar -czf ${TARNAME} ${PKGDIR}
	@rm -rf ${PKGFULLDIR}

.PHONY: docker-build
docker-build:
	rm -rf ${PKGDIR}-*.tar.gz
	docker run --rm -v "$$PWD":/var/ckman -w /var/ckman -e "GO111MODULE=on" eoitek/agent-package:glibc2.12-centos6.8-go1.12.7 make package VERSION=${VERSION}
