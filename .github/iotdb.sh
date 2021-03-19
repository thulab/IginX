#!/bin/sh

set -e

sh -c "wget -nv https://apache.mirror.digionline.de/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip"

sh -c "unzip apache-iotdb-0.11.2-bin.zip"

wait $!

sh -c "ls /"

sh -c "mv apache-iotdb-0.11.2 iotdb"

sh -c "nohup iotdb/sbin/start-server.sh &"
