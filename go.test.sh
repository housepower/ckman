#!/usr/bin/env bash

token=""
hosts=""
resp=""

POST=""
GET=""
PUT=""
DELETE=""

PRINT_TITTLE()
{
    echo "+----------------------------------------------------------+"
    echo "                      $1                                    "
    echo "+----------------------------------------------------------+"

}

GetToken()
{
    PRINT_TITTLE "Login"
    resp=$(curl -s -H "Content-Type: application/json" "localhost:8808/api/login" -d '{"username":"ckman", "password":"63cb91a2ceb9d4f7c8b1ba5e50046f52"}')
    token=$(echo ${resp}|jq '.data.token'|awk -F \" '{print $2}')
    echo ${resp}|jq
    POST="curl -s -H Content-Type:application/json -H token:${token} -X POST -d "
    GET="curl -s -H token:${token} "
    PUT="curl -s -H token:${token} -X PUT "
    DELETE=" curl -s -H token:${token} -X DELETE "
}

#check request result
CheckResult()
{
    echo $2|jq
    retcode=$(echo $2|jq '.code')
    if [[ ${retcode} = "200" ]];then
        echo -e "\033[32m"$1"...[SUCCESS]\033[0m"
    else
        echo -e "\033[31m"$1"...[FAILURE]\033[0m"
        exit 1
    fi
}

ReplaceTemplate()
{
    DOCKER_CLICKHOUSE_HOSTS=$(grep 'DOCKER_CLICKHOUSE_HOSTS' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_CLICKHOUSE_NODE=$(grep 'DOCKER_CLICKHOUSE_NODE' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_SHARDS_REPLICAS=$(grep 'DOCKER_SHARDS_REPLICAS' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_CKNODE=$(grep 'DOCKER_CKNODE' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_ZOOKEEPER_HOSTS=$(grep 'DOCKER_ZOOKEEPER_HOSTS' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_PROM_HOST=$(grep 'DOCKER_PROM_HOST' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    sed -i  "s/{DOCKER_CLICKHOUSE_HOSTS}/${DOCKER_CLICKHOUSE_HOSTS}/g"  `grep -rl '{DOCKER_CLICKHOUSE_HOSTS}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_CLICKHOUSE_NODE}/${DOCKER_CLICKHOUSE_NODE}/g"  `grep -rl '{DOCKER_CLICKHOUSE_NODE}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_CKNODE}/${DOCKER_CKNODE}/g"  `grep -rl '{DOCKER_CKNODE}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_SHARDS_REPLICAS}/${DOCKER_SHARDS_REPLICAS}/g"  `grep -rl '{DOCKER_SHARDS_REPLICAS}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_ZOOKEEPER_HOSTS}/${DOCKER_ZOOKEEPER_HOSTS}/g"  `grep -rl '{DOCKER_ZOOKEEPER_HOSTS}' /tmp/ckman/tests`
    sed -i  "s/127.0.0.1:19090/${DOCKER_PROM_HOST}:9090/g" /tmp/ckman/conf/ckman.yaml

    DOCKER_ZOOKEEPER_HOST=$(echo ${DOCKER_ZOOKEEPER_HOSTS}|awk -F \" '{print $2}')
    DOCKER_CLICKHOUSE_HOST=$(echo ${DOCKER_CLICKHOUSE_HOSTS}|awk -F \" '{print $2}')
    sed -i "s/{DOCKER_ZOOKEEPER_HOST}/${DOCKER_ZOOKEEPER_HOST}/g" /tmp/ckman/tests/conf/metrika.xml
    sed -i "s/{DOCKER_CLICKHOUSE_HOST}/${DOCKER_CLICKHOUSE_HOST}/g" /tmp/ckman/tests/conf/metrika.xml
}

PrepareCKPkg()
{
    cd /tmp/ckman/package
    version=20.9.3.45
    common=clickhouse-common-static-${version}-2.x86_64.rpm
    client=clickhouse-client-${version}-2.noarch.rpm
    server=clickhouse-server-${version}-2.noarch.rpm
    echo "download clickhouse package, version:${version}..."

    # download package from https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/
    curl -s -o ${common} https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/${common}
    echo ${common}" done"
    curl -s -o ${server} https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/${server}
    echo ${server}" done"
    curl -s -o ${client} https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/${client}
    echo ${client}" done"
    cd /tmp/ckman
}

# initalize ckman running environment
Init()
{
    cd /tmp/ckman
    ReplaceTemplate
    bin/start
    PrepareCKPkg
}


Destroy()
{
    cd /tmp/ckman
    bin/stop
}

DeployCKTest()
{
    PRINT_TITTLE "DeployCK"
    resp=$(${POST} @/tmp/ckman/tests/DeployCK.json "localhost:8808/api/v1/deploy/ck")
    CheckResult "DeployCK" "${resp}"
}

DestroyCKTest()
{
    PRINT_TITTLE "DestroyCK"
    resp=$(${PUT} "localhost:8808/api/v1/ck/destroy/test")
    CheckResult "DestoryCK" "${resp}"
}

ImportCkTest()
{
    PRINT_TITTLE "ImportCK"
    resp=$(${POST} @/tmp/ckman/tests/ImportCK.json "localhost:8808/api/v1/ck/cluster")
    CheckResult "ImportCK" "${resp}"
}

DeleteCkTest()
{
    PRINT_TITTLE "DeleteCK"
    resp=$(${DELETE}  "localhost:8808/api/v1/ck/cluster/aa")
    CheckResult "DeleteCK" "${resp}"
}

AddNodeTest()
{
    PRINT_TITTLE "AddNode-"$1
    resp=$(${POST} @/tmp/ckman/tests/AddNode_$1.json "localhost:8808/api/v1/ck/node/test")
    CheckResult "AddNode" "${resp}"
}

DeleteNodeTest()
{
    PRINT_TITTLE "DeleteNode"
    DOCKER_CKNODE=$(grep 'DOCKER_CKNODE' /tmp/ckman/conf/docker_env.conf |awk -F \" '{print $2}')
    resp=$(${DELETE} "localhost:8808/api/v1/ck/node/test?ip=${DOCKER_CKNODE}")
    CheckResult "DeleteNode" "${resp}"
}

SysTest()
{
    GetToken
    ImportCkTest
    DeleteCkTest
    DeployCKTest
    AddNodeTest Replica
    DeleteNodeTest
    AddNodeTest NotReplica
    DestroyCKTest

}

main()
{
    Init

    SysTest

    Destroy
}

# __main__ start
main