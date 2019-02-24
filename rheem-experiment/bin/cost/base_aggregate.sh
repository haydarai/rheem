#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3

NAME="tpch"
CLASS="org.qcri.rheem.apps.tpch.TpcH"

. ./../base/base.sh

. ./../base/execute.sh \
        exp\(1\) ${PLATFORM} \
        file://$(pwd)/../../conf/tpch_${SIZE}_file.properties Q1 #\
#        &> ${FOLDER}/${PLATFORM}_${N_EXECUTION}.log