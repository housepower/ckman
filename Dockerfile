FROM debian:stable-slim

RUN mkdir -p /etc/ckman && cd /etc/ckman && \
    mkdir bin run logs conf package
ADD ./ckman /etc/ckman/bin/ckman
ADD ./cmd/ckmanctl/ckmanctl /etc/ckman/bin/ckmanctl
ADD ./README.md /etc/ckman/package/README.md
ADD ./resources/ckman.hjson /etc/ckman/conf
ADD ./resources/migrate.hjson /etc/ckman/conf
ADD ./resources/password /etc/ckman/conf/password
ADD ./resources/server.key /etc/ckman/conf/server.key
ADD ./resources/server.crt /etc/ckman/conf/server.crt

WORKDIR /etc/ckman
ENTRYPOINT ["bin/ckman"]

