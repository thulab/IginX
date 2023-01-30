#!/bin/sh

set -e

sh -c "wget -nv https://mirror.softaculous.com/apache/zookeeper/zookeeper-3.6.4/apache-zookeeper-3.6.4-bin.tar.gz --no-check-certificate"

sh -c "tar -xzf apache-zookeeper-3.6.4-bin.tar.gz"

sh -c "mv apache-zookeeper-3.6.4-bin zookeeper"

sh -c "cp ./.github/actions/zookeeperRunner/zooMac.cfg zookeeper/conf/zoo.cfg"

sh -c "zookeeper/bin/zkServer.sh start"
