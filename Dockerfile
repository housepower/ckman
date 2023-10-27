FROM debian:stable-slim

RUN mkdir -p /etc/ckman && cd /etc/ckman && \
    mkdir bin run logs conf package
ADD ./ckman /etc/ckman/bin/ckman
ADD ./migrate /etc/ckman/bin/migrate
ADD ./ckmanpasswd /etc/ckman/bin/ckmanpasswd
ADD ./znodefix /etc/ckman/bin/znodefix
ADD ./znode_count /etc/ckman/bin/znode_count
ADD ./README.md /etc/ckman/package/README.md
ADD ./resources/ckman.hjson /etc/ckman/conf
ADD ./resources/migrate.hjson /etc/ckman/conf
ADD ./resources/password /etc/ckman/conf/password
ADD ./resources/server.key /etc/ckman/conf/server.key
ADD ./resources/server.crt /etc/ckman/conf/server.crt

WORKDIR /etc/ckman
ENTRYPOINT ["bin/ckman"]

