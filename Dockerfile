FROM debian:stable-slim

RUN mkdir -p /etc/ckman && cd /etc/ckman && \
    mkdir bin run logs conf package template
ADD ./ckman /etc/ckman/bin/ckman
ADD ./purger /etc/ckman/bin/purger
ADD ./exporter /etc/ckman/bin/exporter
ADD ./rebalancer /etc/ckman/bin/rebalancer
ADD ./schemer /etc/ckman/bin/schemer
ADD ./ckmanpasswd /etc/ckman/bin/ckmanpasswd
ADD ./README.md /etc/ckman/package/README.md
ADD ./resources/config.xml /etc/ckman/template/config.xml
ADD ./resources/users.xml /etc/ckman/template/users.xml
ADD ./resources/ckman.yaml /etc/ckman/conf
ADD ./resources/password /etc/ckman/conf/password
ADD ./resources/server.key /etc/ckman/conf/server.key
ADD ./resources/server.crt /etc/ckman/conf/server.crt

WORKDIR /etc/ckman
ENTRYPOINT ["bin/ckman"]

