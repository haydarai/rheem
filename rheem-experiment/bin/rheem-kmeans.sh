#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2

NAME="kmeans"
BASEDIR="/data/experiments"
FOLDER_CODE="code"
FOLDER_CONF="conf"
FOLDER_LIBS="libs"
CLASS="org.qcri.rheem.apps.kmeans.Kmeans"
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
         exp\(1\) \
         ${PLATFORM} \
         hdfs://10.4.4.30:8300/data/census/${SIZE} \
         3 100 \
        &> ${FOLDER}/${PLATFORM}.log