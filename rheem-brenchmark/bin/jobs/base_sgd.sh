#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
RESTART=$4

NAME="sgd"
CLASS="org.qcri.rheem.apps.sgd.SGD"

. ./../base/base.sh

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

OUTPUT_FILE="/out/${NAME}/${PLATFORM}_${SIZE}_${N_EXECUTION}"

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        preaggregation \
        hdfs://${IP}:8300/data/higgs/${SIZE} \
        ${PARAMETERS} \
        hdfs://${IP}:8300${OUTPUT_FILE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log