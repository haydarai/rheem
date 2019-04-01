#!/usr/bin/env bash

PLATFORM=$1

source $(pwd)/../base/platforms.sh

safe_mode_leave="${HADOOP_HOME}/bin/hdfs dfsadmin -safemode leave"
safe_mode_get="${HADOOP_HOME}/bin/hdfs dfsadmin -safemode get"

#restart spark
result=$(echo ${PLATFORM} | grep "spark")
if [ -n "${result}" ]; then
    echo "restart spark"
    eval ${restart_spark}
fi

#restart flink
result=$(echo ${PLATFORM} | grep "flink")
if [ -n "${result}" ]; then
    echo "restart flink"
    eval ${restart_flink}
fi

#restart java
result=$(echo ${PLATFORM} | grep "java")
if [ -n "${result}" ]; then
    echo "restart java"
    sync; echo 3 > /proc/sys/vm/drop_caches
fi

#restart all
result=$(echo ${PLATFORM} | grep "all")
if [ "${result}" = "all" ]; then
    echo "restarting all"
    echo ${restart_all}
    eval ${restart_all}

#    while [  $(eval ${safe_mode_get} | grep "ON" | wc -l) = "1" ]; do
#        echo "waiting for the safe mode change"
#        eval ${safe_mode_leave}
#        sleep 10
#    done
fi

