#!/bin/sh

set -e

sh -c "wget -nv https://dlcdn.apache.org/iotdb/0.12.2/apache-iotdb-0.12.2-server-bin.zip"

sh -c "unzip apache-iotdb-0.12.2-server-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.2-server-bin"

sh -c "sudo sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb-0.12.2-server-bin/conf/iotdb-engine.properties"

sh -c "sudo cp -r apache-iotdb-0.12.2-server-bin/ apache-iotdb2-0.12.2-server-bin"

sh -c "sudo sed -i 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/g' apache-iotdb2-0.12.2-server-bin/conf/iotdb-engine.properties"

sh -c "sudo sed -i 's/6667/6668/g' apache-iotdb2-0.12.2-server-bin/conf/iotdb-engine.properties"

sudo sh -c "cd apache-iotdb-0.12.2-server-bin/; nohup sbin/start-server.sh &"

sudo sh -c "cd apache-iotdb2-0.12.2-server-bin/; nohup sbin/start-server.sh &"