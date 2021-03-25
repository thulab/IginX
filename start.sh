#!/usr/bin/env bash

mvn clean package -Dmaven.test.skip=true

if [[ -z "${IGINX_HOME}" ]]; then
  export IGINX_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

echo $IGINX_HOME

CLASSPATH=""
for f in ${IGINX_HOME}/core/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

CLASSPATH=${CLASSPATH}":${IGINX_HOME}/iotdb/target/iotdb-0.1.0-SNAPSHOT.jar"":${IGINX_HOME}/influxdb/target/influxdb-0.1.0-SNAPSHOT.jar"

echo $CLASSPATH

if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

MAIN_CLASS=cn.edu.tsinghua.iginx.Iginx

exec "$JAVA" -Duser.timezone=GMT+8 -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?