#!/usr/bin/env bash

if [[ -z "${IGINX_HOME}" ]]; then
  export IGINX_HOME="$(cd "`dirname "$0"`"; pwd)"
fi

echo $IGINX_HOME

CLASSPATH=""
for f in ${IGINX_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

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