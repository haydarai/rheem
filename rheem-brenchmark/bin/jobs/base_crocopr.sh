#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="crocopr"
CLASS="org.qcri.rheem.apps.crocopr.CrocoPR"

. ../base/base.sh

if [ "${PLATFORM}" = "spark" ]; then
    PLATFORM="spark,spark-graph"
fi

if [ "${PLATFORM}" = "java" ]; then
    PLATFORM="java,java-graph"
fi

. ../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        hdfs://${IP}:8300/data/dbpedia_pagelink/${SIZE} \
        hdfs://${IP}:8300/data/dbpedia_pagelink/${SIZE} \
        10 \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log