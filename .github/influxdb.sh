#!/bin/sh

#TODO change the step into iginx, and

set -e

sh -c "wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "tar -zxvf influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "ls"

sh -c "ls influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "sudo cp influxdb2-2.0.7-linux-amd64/{influx,influxd} /usr/local/bin/"

sh -c "wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.7-amd64.deb"

sh -c "sudo dpkg -i influxdb2-2.0.7-amd64.deb"

sh -c "sudo service influxdb start"

sh -c "influx setup --org testOrg --bucket testBucket --username user --password 12345678 --force"

sh -c "influx auth list --json > token.json"

a=$(cat token.json | sed 's/,/\n/g' | grep "token" | sed 's/: /\n/g' | sed '1d' | sed '/^"token/,$d' | sed 's/\"//g')

sed -i "s/your-token/${a}/g" ../conf/config.properties

sed -i "s/my-org/testOrg/g" ../conf/config.properties

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb/#storageEngineList=127.0.0.1#6667#iotdb/g" ../conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#8086#influxdb/storageEngineList=127.0.0.1#8086#influxdb/g" ../conf/config.properties

