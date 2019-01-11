#!/usr/bin/env bash

PLATFORM=$1
CLEAN_CLUSTER=$2

#if not define SPARK_HOME
if [ -z "${SPARK_HOME}" ]; then
    SPARK_HOME=/data/platform/code/processing/spark/spark-2.4.0-bin-hadoop2.6
fi

#if not define HADOOP_HOME
if [ -z "${HADOOP_HOME}" ]; then
    HADOOP_HOME=/data/platform/code/storage/hadoop/hadoop-2.6.5
fi

#if not define FLINK_HOME
if [ -z "${FLINK_HOME}" ]; then
    FLINK_HOME=/data/platform/code/processing/flink/flink-1.7.1
fi


#restart spark
result=$(echo ${PLATFORM} | grep "spark")
if [ -n "${result}" ]; then
   echo "restart spark"
   current_path=$(pwd)
   cd $SPARK_HOME
   . $SPARK_HOME/sbin/stop-all.sh
   . $SPARK_HOME/sbin/start-all.sh
   cd $current_path
fi

#restart flink
result=$(echo ${PLATFORM} | grep "flink")
if [ -n "${result}" ]; then
   echo "restart flink"
   current_path=$(pwd)
   cd $FLINK_HOME/bin
   . $FLINK_HOME/bin/stop-cluster.sh
   . $FLINK_HOME/bin/start-cluster.sh
   cd $current_path
fi

#restart java
result=$(echo ${PLATFORM} | grep "java")
if [ -n "${result}" ]; then
   echo "restart java"
   sync; echo 3 > /proc/sys/vm/drop_caches
fi


if [ "${CLEAN_CLUSTER}" = "all" ]; then
   sync; echo 3 > /proc/sys/vm/drop_caches
   ssh root@10.4.4.32 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.35 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.33 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.25 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.36 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.23 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.34 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.29 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.28 "sync; echo 3 > /proc/sys/vm/drop_caches"
   ssh root@10.4.4.24 "sync; echo 3 > /proc/sys/vm/drop_caches"
fi

