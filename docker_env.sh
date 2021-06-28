#!/usr/bin/env bash

# This script is running for test-ci, after docker-compose up, run it to get container host.

OUTFILES=/tmp/ckman/conf/docker_env.conf

if [[ ! -d /tmp/ckman ]];then
    mkdir -p /tmp/ckman/conf
fi

rm -rf ${OUTFILES}

# be used for test DeployCK
DOCKER_CLICKHOUSE_NODES=""

# be used for test AddNode
DOCKER_CKNODE=""

#zookeeper host
DOCKER_ZOOKEEPER_HOSTS=""

node1=$(docker ps -a |grep ckman_cknode_1 |awk '{print $1}')
DOCKER_NODE1=$(docker exec $node1 cat /etc/hosts |grep $node1| awk '{print $1}')

node2=$(docker ps -a |grep ckman_cknode_2 |awk '{print $1}')
DOCKER_NODE2=$(docker exec $node2 cat /etc/hosts |grep $node2| awk '{print $1}')

node3=$(docker ps -a |grep ckman_cknode_3 |awk '{print $1}')
DOCKER_NODE3=$(docker exec $node3 cat /etc/hosts |grep $node3| awk '{print $1}')


node4=$(docker ps -a |grep ckman_cknode_4 |awk '{print $1}')
DOCKER_NODE4=$(docker exec $node4 cat /etc/hosts |grep $node4| awk '{print $1}')

DOCKER_CLICKHOUSE_NODES="[\"${DOCKER_NODE1}\",\"${DOCKER_NODE2}\",\"${DOCKER_NODE3}\",\"${DOCKER_NODE4}\"]"

zk=$(docker ps -a |grep zookeeper |awk '{print $1}')
DOCKER_ZOOKEEPER_HOSTS=$(docker exec $zk cat /etc/hosts |grep $zk| awk '{print $1}')
DOCKER_ZOOKEEPER_HOSTS="[\"${DOCKER_ZOOKEEPER_HOSTS}\"]"


echo "DOCKER_NODE1="${DOCKER_NODE1} >> ${OUTFILES}
echo "DOCKER_NODE2="${DOCKER_NODE2} >> ${OUTFILES}
echo "DOCKER_NODE3="${DOCKER_NODE3} >> ${OUTFILES}
echo "DOCKER_NODE4="${DOCKER_NODE4} >> ${OUTFILES}
echo "DOCKER_CLICKHOUSE_NODES="${DOCKER_CLICKHOUSE_NODES} >> ${OUTFILES}
echo "DOCKER_ZOOKEEPER_HOSTS="${DOCKER_ZOOKEEPER_HOSTS} >> ${OUTFILES}
