#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="wordcount"
CLASS="org.qcri.rheem.apps.wordcount.Main"

. ./../base/base.sh

OUTPUT_FILE="/out/${NAME}/${PLATFORM}_${SIZE}_${N_EXECUTION}"

. ./../base/execute.sh \
        ${PLATFORM} \
        hdfs://${IP}:8300/data/long_abstract_clean/${SIZE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log