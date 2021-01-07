#!/usr/bin/env bash

# This script is running for test-ci, after docker-compose up, run it to get container host.

OUTFILES=/tmp/ckman/conf/docker_env.conf

if [[ ! -d /tmp/ckman ]];then
    mkdir /tmp/ckman
fi

rm -rf ${OUTFILES}

# be used for test ImportCK
DOCKER_CLICKHOUSE_HOSTS=""

# be used for test DeployCK
DOCKER_CLICKHOUSE_NODES=""
DOCKER_SHARDS_REPLICAS=""

# be used for test AddNode
DOCKER_CKNODE=""

#zookeeper host
DOCKER_ZOOKEEPER_HOSTS=""

#promethues host
DOCKER_PROM_HOST=""

for ck in $(docker ps -a |grep clickhouse-server |awk '{print $1}')
do
    CK_HOST=$(docker inspect ${ck}|grep IPAddress|grep -v \"\" |grep -v null|awk -F : '{print $2}')
    DOCKER_CLICKHOUSE_HOSTS=${DOCKER_CLICKHOUSE_HOSTS}${CK_HOST}
done
DOCKER_CLICKHOUSE_HOSTS=$(echo ${DOCKER_CLICKHOUSE_HOSTS}|sed 's/.$//')
DOCKER_CLICKHOUSE_HOSTS="["${DOCKER_CLICKHOUSE_HOSTS}"]"

node=$(docker ps -a |grep ckman_cknode_1 |awk '{print $1}')
DOCKER_CLICKHOUSE_NODE=$(docker inspect ${node}|grep IPAddress|grep -v \"\" |grep -v null|cut -d ':' -f 2 |cut -d ',' -f 1)
DOCKER_SHARDS_REPLICAS=${DOCKER_CLICKHOUSE_NODE}
DOCKER_CLICKHOUSE_NODE="["${DOCKER_CLICKHOUSE_NODE}"]"

cknode=$(docker ps -a |grep ckman_cknode_2 |awk '{print $1}')
DOCKER_CKNODE=$(docker inspect ${cknode}|grep IPAddress|grep -v \"\" |grep -v null|cut -d ':' -f 2 |cut -d ',' -f 1)

for zk in $(docker ps -a |grep zookeeper |awk '{print $1}')
do
    ZK_HOST=$(docker inspect ${zk}|grep IPAddress|grep -v \"\" |grep -v null|awk -F : '{print $2}')
    DOCKER_ZOOKEEPER_HOSTS=${DOCKER_ZOOKEEPER_HOSTS}${ZK_HOST}
done
DOCKER_ZOOKEEPER_HOSTS=$(echo ${DOCKER_ZOOKEEPER_HOSTS}|sed 's/.$//')
DOCKER_ZOOKEEPER_HOSTS="["${DOCKER_ZOOKEEPER_HOSTS}"]"

prom=$(docker ps -a |grep prometheus |awk '{print $1}')
DOCKER_PROM_HOST=$(docker inspect ${prom}|grep IPAddress|grep -v \"\" |grep -v null|awk -F '"' '{print $(NF-1)}')

echo "DOCKER_CLICKHOUSE_HOSTS="${DOCKER_CLICKHOUSE_HOSTS} >> ${OUTFILES}
echo "DOCKER_CLICKHOUSE_NODE="${DOCKER_CLICKHOUSE_NODE} >> ${OUTFILES}
echo "DOCKER_SHARDS_REPLICAS="${DOCKER_SHARDS_REPLICAS} >> ${OUTFILES}
echo "DOCKER_CKNODE="${DOCKER_CKNODE} >> ${OUTFILES}
echo "DOCKER_ZOOKEEPER_HOSTS="${DOCKER_ZOOKEEPER_HOSTS} >> ${OUTFILES}
echo "DOCKER_PROM_HOST="${DOCKER_PROM_HOST} >> ${OUTFILES}
