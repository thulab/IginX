#!/bin/sh

set -e

sed -i "s/storageEngineList=127.0.0.1#6667#iotdb11/#storageEngineList=127.0.0.1#6667#iotdb11/g" conf/config.properties

sed -i "s/#storageEngineList=127.0.0.1#6667#parquet/storageEngineList=127.0.0.1#6667#parquet/g" conf/config.properties

sed -i "s/enablePushDown=true/enablePushDown=false/g" conf/config.properties
