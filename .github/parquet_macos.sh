#!/bin/sh

set -e

sed -i "" "s/storageEngineList=127.0.0.1#6667/#storageEngineList=127.0.0.1#6667/" conf/config.properties

sed -i "" "s/#storageEngineList=127.0.0.1#6667#parquet#dir=parquetData#isLocal=true/storageEngineList=127.0.0.1#6667#parquet#dir=parquetData#isLocal=true/" conf/config.properties

sed -i "" "s/enablePushDown=true/enablePushDown=false/" conf/config.properties
