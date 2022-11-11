#!/bin/sh

mkdir -p /var/log/ckman /run/ckman
chown -R ckman:ckman /var/log/ckman /etc/ckman /run/ckman
chmod 750 /var/log/ckman /etc/ckman

/bin/systemctl daemon-reload
/bin/systemctl enable ckman

if [ -f /etc/ckman/conf/ckman.yaml ]; then
	/usr/local/bin/yaml2json /etc/ckman/conf/ckman.yaml > /etc/ckman/conf/ckman.hjson
	mv /etc/ckman/conf/ckman.yaml /etc/ckman/conf/ckman.yaml.rpmsave
fi