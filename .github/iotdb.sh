#!/bin/sh

set -e

sh -c "wget -nv https://mirrors.ocf.berkeley.edu/apache/iotdb/0.11.3/apache-iotdb-0.11.3-bin.zip"

sh -c "echo ========================="

sh -c "unzip apache-iotdb-0.11.3-bin.zip"

sh -c "sleep 10"

sh -c "sudo cp -r apache-iotdb-0.11.3/ apache-iotdb2-0.11.3/"

sh -c "sudo sed -i 's/6667/6668/g' apache-iotdb2-0.11.3/conf/iotdb-engine.properties"

sh -c "cd apache-iotdb-0.11.3/; nohup sbin/start-server.sh &"

sh -c "cd apache-iotdb2-0.11.3/; nohup sbin/start-server.sh &"

