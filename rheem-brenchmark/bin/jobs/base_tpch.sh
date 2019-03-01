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

. ./../base/execute.sh \
        exp\(1\) \
        ${PLATFORM} \
        file://${BASEDIR}/${FOLDER_CONF}/tpch_${SIZE}_file.properties \
        ${QUERY} \
        &> ${FOLDER}/${PLATFORM}_${SIZE}_${N_EXECUTION}.log