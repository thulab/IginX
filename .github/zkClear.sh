#!/bin/sh

set -e

sh -c "zookeeper/bin/zkServer.sh stop"

sh -c "sleep 2"

sh -c "rm -r /tmp/zookeeper"

sh -c "mkdir /tmp/zookeeper"
