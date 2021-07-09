#!/bin/sh

set -e

sh -c "wget -nv https://mirrors.ocf.berkeley.edu/apache/iotdb/0.11.4/apache-iotdb-0.11.4-bin.zip"

sh -c "unzip apache-iotdb-0.11.4-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.11.4"

sh -c "nohup apache-iotdb-0.11.4/sbin/start-server.sh &"
