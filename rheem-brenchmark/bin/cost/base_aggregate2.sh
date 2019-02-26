#!/usr/bin/env bash

PLATFORM=$1
SIZE=$2
N_EXECUTION=$3

NAME="tpch"
CLASS="org.qcri.rheem.experiment.ExperimentExecutor"

. ./../base/base.sh

. ./../base/execute.sh \
        -exn ${NAME} \
        -plat ${PLATFORM} \
        -q "Q1" \
        -lineitem hdfs://10.4.4.32:8300/data/${NAME}/lineitem/200 \
        -orders hdfs://10.4.4.32:8300/data/${NAME}/orders/200 \
        -customer hdfs://10.4.4.32:8300/data/${NAME}/customer/200 \
        -o hdfs://10.4.4.32:8300/output/${NAME}/${PLATFORM}/${SIZE} \
        -start "1998-12-01" \
        -delta 90 \
        &> ${FOLDER}/${PLATFORM}_${N_EXECUTION}.log

#        &> ${FOLDER}/${PLATFORM}.log
#        exp\(1\) ${PLATFORM} \
#        file://$(pwd)/../../conf/tpch_${SIZE}_file.properties Q1 #\
