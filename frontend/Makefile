DEST?=dist
ENV?=dev
dev:
	node_modules/.bin/vue-cli-service serve src/main.tsx --open --proxy-env=${ENV}

clear:
	rm -rf ${DEST}/*

build: clear
	node_modules/.bin/vue-cli-service build src/main.tsx --dest ${DEST}

lint:
	node_modules/.bin/vue-cli-service lint src/main.tsx

install:
	yarn install --registry=https://registry.npm.taobao.org

tar:
	tar --exclude="dist/.DS_Store" -zcvf loganalysis-$(shell git describe --tags --long)-$(shell date "+%Y%m%d").tar.gz dist

ftp:
	ncftpput 192.168.31.84 /packages/newlook loganalysis-*.tar.gz

sonar:
	sonar-scanner -Dsonar.projectKey=log-analysis -Dsonar.sources=. -Dsonar.host.url=https://sonar.eoitek.net
