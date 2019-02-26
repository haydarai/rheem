#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3

NAME="crocopr"
CLASS="org.qcri.rheem.apps.crocopr.CrocoPR"

. ../base/base.sh
#hdfs dfs -rm -r -skipTrash /output/crocopr/0010

if [ "${PLATFORM}" = "spark" ]; then
    PLATFORM="spark,spark-graph"
fi


. ../base/execute.sh \
        exp\(1\) ${PLATFORM} \
        hdfs://${IP}:8300/data/dbpedia_pagelink/${SIZE} \
        hdfs://${IP}:8300/data/dbpedia_pagelink/${SIZE} \
        10 \
        &> ${FOLDER}/${PLATFORM}_${N_EXECUTION}.log
