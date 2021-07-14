#!/bin/sh

set -e

sh -c "wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "tar -zxvf influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "ls influxdb2-2.0.7-linux-amd64"

sh -c "sudo cp -r influxdb2-2.0.7-linux-amd64/ influxdb2-2.0.7-linux-amd64-2/"

sudo sh -c "cd influxdb2-2.0.7-linux-amd64/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:8086 &"

sh -c "./influxdb2-2.0.7-linux-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --force"

sh -c "./influxdb2-2.0.7-linux-amd64/influx auth list --json > token.json"

a=$(cat token.json | sed 's/,/\n/g' | grep "token" | sed 's/: /\n/g' | sed '1d' | sed '/^"token/,$d' | sed 's/\"//g')

sed -i "s/your-token/${a}/g" conf/config.properties

sed -i "s/my-org/testOrg/g" conf/config.properties

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb/#storageEngineList=127.0.0.1#6667#iotdb/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#8086#influxdb/storageEngineList=127.0.0.1#8086#influxdb/g" conf/config.properties

sudo sh -c "cd influxdb2-2.0.7-linux-amd64-2/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt2 --engine-path=~/.influxdbv2/engine2 --http-bind-address=:8087 &"