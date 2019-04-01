#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="kmeans"
CLASS="org.qcri.rheem.apps.kmeans.Kmeans"

. ./../base/base.sh

OUTPUT_FILE="/out/${NAME}/${PLATFORM}_${SIZE}_${N_EXECUTION}"

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        hdfs://10.4.4.30:8300/data/census/${SIZE} \
        3 \
        100 \
        hdfs://${IP}:8300${OUTPUT_FILE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log