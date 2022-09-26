#!/bin/sh

set -e

sh -c "wget -nv https://github.com/thulab/IginX-benchmarks/raw/main/resources/apache-iotdb-0.12.6-server-bin.zip"

sh -c "unzip apache-iotdb-0.12.6-server-bin.zip"

sh -c "sleep 10"

sh -c "ls ./"

sh -c "echo ========================="

sh -c "ls apache-iotdb-0.12.6-server-bin"

sh -c "sudo sed -i '' 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/' apache-iotdb-0.12.6-server-bin/conf/iotdb-engine.properties"

sh -c "sudo cp -r apache-iotdb-0.12.6-server-bin/ apache-iotdb2-0.12.6-server-bin"

sh -c "sudo sed -i '' 's/# wal_buffer_size=16777216/wal_buffer_size=167772160/' apache-iotdb2-0.12.6-server-bin/conf/iotdb-engine.properties"

sh -c "sudo sed -i '' 's/6667/6668/' apache-iotdb2-0.12.6-server-bin/conf/iotdb-engine.properties"

sudo sh -c "cd apache-iotdb-0.12.6-server-bin/; nohup sbin/start-server.sh &"

sudo sh -c "cd apache-iotdb2-0.12.6-server-bin/; nohup sbin/start-server.sh &"

sed -i "" "s/iotdb11/iotdb12/" conf/config.properties
