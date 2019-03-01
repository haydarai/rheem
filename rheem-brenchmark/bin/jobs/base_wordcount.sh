#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="wordcount"
CLASS="org.qcri.rheem.apps.wordcount.WordCountScala"

. ./../base/base.sh

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        hdfs://${IP}:8300/data/long_abstract_clean/${SIZE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log