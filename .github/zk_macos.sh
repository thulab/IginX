#!/bin/sh

set -e

sh -c "wget -nv https://mirror.softaculous.com/apache/zookeeper/zookeeper-3.6.3/apache-zookeeper-3.6.3-bin.tar.gz --no-check-certificate"

sh -c "tar -xzf apache-zookeeper-3.6.3-bin.tar.gz"

sh -c "mv apache-zookeeper-3.6.3-bin zookeeper"

sh -c "cp zookeeper/conf/zoo_sample.cfg zookeeper/conf/zoo.cfg"

sh -c "sed -i '' 's/#maxClientCnxns=60/maxClientCnxns=60/' zookeeper/conf/zoo.cfg"

sh -c "cat zookeeper/conf/zoo.cfg"

sh -c "zookeeper/bin/zkServer.sh start"

sh -c "zookeeper/bin/zkCli.sh ls /"
