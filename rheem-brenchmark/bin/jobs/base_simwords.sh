#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="simwords"
CLASS="org.qcri.rheem.apps.simwords.SimWords"

. ./../base/base.sh

OUTPUT_FILE="/out/${NAME}/${PLATFORM}_${SIZE}_${N_EXECUTION}"

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        hdfs://${IP}:8300/data/long_abstract_clean/${SIZE} \
        10 \
        2 \
        10 \
        1 \
        hdfs://${IP}:8300${OUTPUT_FILE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log