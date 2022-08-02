#!/bin/sh

mkdir -p /var/log/ckman /run/ckman
chown -R ckman:ckman /var/log/ckman /etc/ckman /run/ckman
chmod 750 /var/log/ckman /etc/ckman

/bin/systemctl daemon-reload
/bin/systemctl enable ckman
