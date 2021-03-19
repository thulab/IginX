#!/bin/sh

set -e

sh -c "wget -nv https://mirror.softaculous.com/apache/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz"

sh -c "tar -xzf apache-zookeeper-3.6.2-bin.tar.gz"

sh -c "mv apache-zookeeper-3.6.2-bin zookeeper"

sh -c "cp zookeeper/conf/zoo_sample.cfg zookeeper/conf/zoo.cfg"

sh -c "zookeeper/bin/zkServer.sh start"
