#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="kmeans"
CLASS="org.qcri.rheem.apps.kmeans.Kmeans"

. ./../base/base.sh

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        hdfs://10.4.4.30:8300/data/census/${SIZE} \
        3 \
        100 \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log