#!/bin/sh

set -e

sh -c "zookeeper/bin/zkServer.sh stop"

sh -c "sleep 2"

sh -c "rm -rf zookeeper/data"

sh -c "mkdir zookeeper/data"

sh -c "rm -rf zookeeper/logs"

sh -c "mkdir zookeeper/logs"
