#!/usr/bin/env bash

if [ "${CONTEXT}" = "true" ]; then
    echo "java ${FLAGS} -cp \"${RHEEM_CLASSPATH}\" ${CLASS} $*"
    #with time out of 1000 second
    #timeout 1000 java$FLAGS -cp "${RHEEM_CLASSPATH}" $CLASS $*
    #without time out
    java ${FLAGS} -cp "${RHEEM_CLASSPATH}" ${CLASS} $*
else
    echo "Error the context is not define"
    exit 1
fi