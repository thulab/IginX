#!/bin/sh

set -e

sh -c "wget -nv https://mirrors.ocf.berkeley.edu/apache/iotdb/0.11.4/apache-iotdb-0.11.4-bin.zip"

sh -c "unzip apache-iotdb-0.11.4-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.11.4"

sh -c "cp -r apache-iotdb-0.11.4/ apache-iotdb2-0.11.4/"

sh -c "sed -i 's/6667/6668/g' apache-iotdb2-0.11.4/conf/iotdb-engine.properties"

sh -c "cd apache-iotdb-0.11.4/; nohup sbin/start-server.sh &"

sh -c "cd apache-iotdb2-0.11.4/; nohup sbin/start-server.sh &"
