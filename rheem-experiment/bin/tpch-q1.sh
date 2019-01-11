#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2

NAME="tpch"
BASEDIR="/data/experiments"
FOLDER_CODE="code"
FOLDER_CONF="conf"
FOLDER_LIBS="libs"
CLASS="org.qcri.rheem.experiment.ExperimentExecutor"
FLAG_AKKA="true"
FLAG_LOG="true"
FLAG_RHEEM="true"
FLAG_FLINK="true"
FLAG_SPARK="true"
OTHER_FLAGS="-Xms10g -Xmx10g -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:ParallelGCThreads=4"

/data/platform/code/storage/hadoop/hadoop-2.6.5/bin/hdfs dfs -rm -r -skipTrash /output/${NAME}/${PLATFORM}/${SIZE}

FOLDER=$(pwd)/../logs/${NAME}/${SIZE}
if [ ! -d "${FOLDER}" ]; then
    mkdir -p ${FOLDER}
fi

. ./enviroment.sh

. ./execute.sh \
        -exn ${NAME} \
        -plat ${PLATFORM} \
        -q "Q1" \
        -lineitem hdfs://10.4.4.32:8300/data/${NAME}/lineitem/${SIZE} \
        -orders hdfs://10.4.4.32:8300/data/${NAME}/orders/${SIZE} \
        -customer hdfs://10.4.4.32:8300/data/${NAME}/customer/${SIZE} \
        -o hdfs://10.4.4.32:8300/output/${NAME}/${PLATFORM}/${SIZE} \
        -start "1998-12-01" \
        -delta 90 \
        &> ${FOLDER}/${PLATFORM}.log