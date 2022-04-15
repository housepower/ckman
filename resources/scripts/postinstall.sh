#!/bin/sh

mkdir -p /var/log/ckman /run/ckman
chown -R ckman:ckman /var/log/ckman /etc/ckman

chmod -R 664 /var/log/ckman /etc/ckman
chmod 750 /var/log/ckman /etc/ckman /etc/ckman/conf /etc/ckman/package /etc/ckman/dbscript

/bin/systemctl daemon-reload
/bin/systemctl enable ckman
