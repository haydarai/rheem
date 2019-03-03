#!/usr/bin/env bash


PLATFORM=$1
SIZE=$2
N_EXECUTION=$3
QUERY=$4
RESTART=$5

NAME="tpch"
CLASS="org.qcri.rheem.apps.tpch.TpcH"

. ./../base/base.sh

getFromTemplate ${BASEDIR}/${FOLDER_CONF}/template/tpch_${SIZE}_file.properties ${BASEDIR}/${FOLDER_CONF}/tpch_${SIZE}_file.properties

OUTPUT_FILE="/out/${NAME}/${QUERY}/${PLATFORM}_${SIZE}_${N_EXECUTION}"

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        file://${BASEDIR}/${FOLDER_CONF}/tpch_${SIZE}_file.properties \
        ${QUERY} \
        hdfs://${IP}:8300${OUTPUT_FILE} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log