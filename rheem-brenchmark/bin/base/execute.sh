#!/usr/bin/env bash

if [ "${CONTEXT}" = "true" ]; then
    if [ "${RESTART}" = "true" ]; then
        echo "restarting the platforms"
        . ../base/restart.sh all
    fi

    if [ -z "${OUTPUT_FILE}" ] ;then
        echo "deleting the file ${OUTPUT_FILE} in hdfs"
        eval "${HDFS_DELETE} ${OUTPUT_FILE}"
    fi

    echo "java ${FLAGS} -cp \"${RHEEM_CLASSPATH}\" ${CLASS} $*"

    if [ -z "${TIMEOUT}" ]; then
        echo "timeout of ${TIMEOUT}"
        #with time out of using the TIMEOUT variable second
        timeout ${TIMEOUT} java ${FLAGS} -cp "${RHEEM_CLASSPATH}" ${CLASS} $*
        status=$(echo $?)
        if [ "$(status)" = "0" ]; then
            echo "$* OK" >> outtime.log
        else
            echo "$* error $status" >> outtime.log
        fi
    else
        #without time out
        java ${FLAGS} -cp "${RHEEM_CLASSPATH}" ${CLASS} $*
    fi
else
    echo "Error the context is not define"
    exit 1
fi