#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2

NAME="sgd"
BASEDIR="/data/experiments"
FOLDER_CODE="code"
FOLDER_CONF="conf"
FOLDER_LIBS="libs"
CLASS="org.qcri.rheem.apps.sgd.SGD"
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

. enviroment.sh
PARAMETERS=""

if [ "${SIZE}" = "0010" ]; then
    PARAMETERS="1100000 28 1000 0 1"
fi

if [ "${SIZE}" = "0025" ]; then
    PARAMETERS="2750000 28 1000 0 1"
fi

if [ "${SIZE}" = "0050" ]; then
    PARAMETERS="5500000 28 1000 0 1"
fi


if [ "${SIZE}" = "0100" ]; then
    PARAMETERS="11000000 28 1000 0 1"
fi


if [ "${SIZE}" = "0200" ]; then
    PARAMETERS="22000000 28 1000 0 1"
fi

if [ "${SIZE}" = "0400" ]; then
    PARAMETERS="44000000 28 1000 0 1"
fi

. execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        preaggregation \
        hdfs://10.4.4.32:8300/data/higgs/${SIZE} \
        ${PARAMETERS} \
        &> ${FOLDER}/${PLATFORM}.log