#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="word2nvec"
CLASS="org.qcri.rheem.apps.simwords.Word2NVec"

. ./../base/base.sh

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        hdfs://${IP}:8300/data/long_abstract_clean/${SIZE} \
        10 \
        2 \
        hdfs://${IP}:8300/output/${NAME}/${PLATFORM}/${SIZE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log