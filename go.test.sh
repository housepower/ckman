#!/usr/bin/env bash

token=""
hosts=""
resp=""

POST=""
GET=""
PUT=""
DELETE=""
DOCKER_NODE4=""

PRINT_TITTLE()
{
    echo "+----------------------------------------------------------+"
    echo "                      $1                                    "
    echo "+----------------------------------------------------------+"

}

GetToken()
{
    PRINT_TITTLE "Login"
    resp=$(curl -s -H "Content-Type: application/json" "localhost:18808/api/login" -d '{"username":"ckman", "password":"63cb91a2ceb9d4f7c8b1ba5e50046f52"}')
    token=$(echo ${resp}|jq '.entity.token'|awk -F \" '{print $2}')
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
    retcode=$(echo $2|jq '.retCode')
    if [[ ${retcode} = "0" ]];then
        echo -e "\033[32m"$1"...[SUCCESS]\033[0m"
    else
        echo -e "\033[31m"$1"...[FAILURE]\033[0m"
	Destroy
        exit 1
    fi
}

ReplaceTemplate()
{
    DOCKER_NODE1=$(grep 'DOCKER_NODE1' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_NODE2=$(grep 'DOCKER_NODE2' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_NODE3=$(grep 'DOCKER_NODE3' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_NODE4=$(grep 'DOCKER_NODE4' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_CLICKHOUSE_NODES=$(grep 'DOCKER_CLICKHOUSE_NODES' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    DOCKER_ZOOKEEPER_HOSTS=$(grep 'DOCKER_ZOOKEEPER_HOSTS' /tmp/ckman/conf/docker_env.conf |awk -F = '{print $2}')
    sed -i  "s/{DOCKER_NODE1}/${DOCKER_NODE1}/g"  `grep -rl '{DOCKER_NODE1}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_NODE2}/${DOCKER_NODE2}/g"  `grep -rl '{DOCKER_NODE2}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_NODE3}/${DOCKER_NODE3}/g"  `grep -rl '{DOCKER_NODE3}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_NODE4}/${DOCKER_NODE4}/g"  `grep -rl '{DOCKER_NODE4}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_CLICKHOUSE_NODES}/${DOCKER_CLICKHOUSE_NODES}/g"  `grep -rl '{DOCKER_CLICKHOUSE_NODES}' /tmp/ckman/tests`
    sed -i  "s/{DOCKER_ZOOKEEPER_HOSTS}/${DOCKER_ZOOKEEPER_HOSTS}/g"  `grep -rl '{DOCKER_ZOOKEEPER_HOSTS}' /tmp/ckman/tests`
    sed -i  's/port: 8808/port: 18808/g' /tmp/ckman/conf/ckman.yaml
    sed -i  's#<listen_host>{{.CkListenHost}}</listen_host>#<listen_host>0.0.0.0</listen_host>#g' /tmp/ckman/template/config.xml
}

PrepareCKPkg()
{
    cd /tmp/ckman/package
    version=$1
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

Installjq()
{
   if test $(which jq) = "" ; then
       yum install -y epel-release > /dev/null
       yum install -y jq > /dev/null
   fi
}

# initalize ckman running environment
Init()
{
    if test $(uname) != "Linux"; then
        echo "This platform not support this script."
        exit 1
    fi
    cd /tmp/ckman
    ReplaceTemplate
    bin/stop 2>/dev/null
    PrepareCKPkg 20.9.3.45
    PrepareCKPkg 21.3.9.83
    Installjq
    bin/start
    sleep 1
}


Destroy()
{
    cd /tmp/ckman
    bin/stop
}

DeployCKTest()
{
    PRINT_TITTLE "DeployCK"
    resp=$(${POST} @/tmp/ckman/tests/DeployCK.json "localhost:18808/api/v1/deploy/ck")
    CheckResult "DeployCK" "${resp}"
}

DestroyCKTest()
{
    PRINT_TITTLE "DestroyCK"
    resp=$(${PUT} "localhost:18808/api/v1/ck/destroy/test")
    CheckResult "DestoryCK" "${resp}"
}

AddNodeTest()
{
    PRINT_TITTLE "AddNode"
    resp=$(${POST} @/tmp/ckman/tests/AddNode.json "localhost:18808/api/v1/ck/node/test")
    CheckResult "AddNode" "${resp}"
}

DeleteNodeTest()
{
    PRINT_TITTLE "DeleteNode"
    DOCKER_CKNODE=$(grep 'DOCKER_CKNODE' /tmp/ckman/conf/docker_env.conf |awk -F \" '{print $2}')
    resp=$(${DELETE} "localhost:18808/api/v1/ck/node/test?ip=${DOCKER_NODE4}")
    CheckResult "DeleteNode" "${resp}"
}

UpgradeTest()
{
    PRINT_TITTLE "Upgrade"
    resp=$(${PUT} -d @/tmp/ckman/tests/Upgrade.json "localhost:18808/api/v1/ck/upgrade/test")
    CheckResult "Upgrade" "${resp}"

}

SysTest()
{
    GetToken
    DeployCKTest
    DeleteNodeTest
    AddNodeTest 
    UpgradeTest
    #DestroyCKTest
}

main()
{
    Init

    SysTest

    Destroy
}

# __main__ start
main
