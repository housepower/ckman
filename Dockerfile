# This Dockerfile is used for build a docker container to build ckman project which in Linux.
# You can run command like: "docker build -t ckman-build:go-1.15.3 ."
# the offical image is eoitek/ckman-build:go-1.15.3, You can pull it from dockerhub.

FROM amd64/golang:1.15.3

WORKDIR /var/
RUN apt-get update && apt-get install -y jq \
    && wget https://nodejs.org/download/release/v14.15.3/node-v14.15.3-linux-x64.tar.gz \
    && tar -xzvf node-v14.15.3-linux-x64.tar.gz -C /usr/local/ \
    && ln -s /usr/local/node-v14.15.3-linux-x64/bin/node /usr/local/bin \
    && ln -s /usr/local/node-v14.15.3-linux-x64/bin/npm /usr/local/bin \
    && cd /go && go get github.com/markbates/pkger/cmd/pkger

COPY frontend/package.json .
RUN npm install