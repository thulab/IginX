#!/bin/sh

set -e

sh -c "wget -nv https://mirrors.ocf.berkeley.edu/apache/iotdb/0.12.2/apache-iotdb-0.12.2-server-bin.zip"

sh -c "unzip apache-iotdb-0.12.2-server-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.2-server-bin"

sh -c "sudo cp -r apache-iotdb-0.12.2-server-bin/ apache-iotdb2-0.12.2-server-bin/"

sh -c "sudo sed -i 's/6667/6668/g' apache-iotdb2-0.12.2-server-bin/conf/iotdb-engine.properties"

sh -c "sudo sed -i 's/# enable_wal=false/enable_wal=false/' apache-iotdb-0.12.2-server-bin/conf/iotdb-engine.properties"

sh -c "sudo sed -i 's/# enable_wal=false/enable_wal=false/' apache-iotdb2-0.12.2-server-bin/conf/iotdb-engine.properties"

sudo sh -c "cd apache-iotdb-0.12.2-server-bin/; nohup sbin/start-server.sh &"

sudo sh -c "cd apache-iotdb2-0.12.2-server-bin/; nohup sbin/start-server.sh &"
