#!/bin/sh

set -e

sh -c "zookeeper/bin/zkServer.sh stop"

sh -c "sleep 2"

sh -c "rm -r zookeeper/data"

sh -c "mkdir zookeeper/data"
