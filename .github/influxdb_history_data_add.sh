#!/bin/sh

set -e

sh -c "wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "tar -zxvf influxdb2-2.0.7-linux-amd64.tar.gz"

sh -c "ls influxdb2-2.0.7-linux-amd64"

sudo sh -c "cd influxdb2-2.0.7-linux-amd64/; nohup ./influxd run --bolt-path=~/.influxdbv2/influxd.bolt --engine-path=~/.influxdbv2/engine --http-bind-address=:8086 --query-memory-bytes=20971520 &"

sh -c "sleep 30"

sh -c "./influxdb2-2.0.7-linux-amd64/influx setup --org testOrg --bucket testBucket --username user --password 12345678 --token testToken --force"

sed -i "s/enablePushDown=true/enablePushDown=false/g" conf/config.properties
