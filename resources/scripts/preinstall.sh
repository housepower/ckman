#!/bin/sh

if test ! $(grep ckman /etc/passwd); then
    useradd ckman
    echo "user ckman created"
fi
