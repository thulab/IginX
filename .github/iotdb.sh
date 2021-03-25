#!/bin/sh

set -e

sh -c "wget -nv https://apache.mirror.digionline.de/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip"

sh -c "unzip apache-iotdb-0.11.2-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.11.2"

sh -c "nohup apache-iotdb-0.11.2/sbin/start-server.sh &"
