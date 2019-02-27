#!/usr/bin/env bash

PLATFORMS=$1
SIZE_PLAN_MIN=$2
SIZE_PLAN_MAX=$3
CARDINALITY=$4
DATASIZE=$5
TYPE=$6
EXEC_NAME=$7
SUBFIX=$8
RESTART=$9

NAME="ml-gen-${EXEC_NAME}"
CLASS="org.qcri.rheem.profiler.core.api.ProfilingApp"

. ../base/base.sh

if [ -z "${RESTART}" ]; then
    RESTART="false"
fi

if [ "${RESTART}" = "true" ]; then
   . ../base/restart.sh all
fi

. ../base/execute.sh \
    ${TYPE} \
    ${SIZE_PLAN_MIN} \
    ${SIZE_PLAN_MAX} \
    ${CARDINALITY} \
    ${DATASIZE} \
    ${PLATFORMS} \
    |& tee ${FOLDER}/${NAME}_${SIZE_PLAN_MIN}-${SIZE_PLAN_MAX}_${PLATFORMS}_${SUBFIX}.log








#!/bin/bash
#PLATFORMS=("spark" "java,spark" "java")
#NAME=planProfiling


#for PLATFORM in ${PLATFORMS[@]}
#do
#Set environment vars
#echo "Setting environment variables!"
#. /root/anis/conf/set-env.sh

#for i in {1..9}
#do
#   ./scripts/restart.sh spark ${PLATFORM} all ${NAME}
#   ./scripts/profilerApp exhaustive_profiling $i $i 1,100,1000,10000,100000 1,10,100,1000 ${PLATFORM} |& tee logs/profiling/${NAME}_${i}-${i}_${PLATFORM}_SGDoperators+lowcard.log
#   ./scripts/restart.sh spark ${PLATFORM} all ${NAME}
#   ./scripts/profilerApp exhaustive_profiling $i $i 1000000 1,10,100 ${PLATFORM} |& tee logs/profiling/${NAME}_${i}-${i}_${PLATFORM}_SGDoperators+lowMedcard.log
#   ./scripts/restart.sh spark ${PLATFORM} all ${NAME}
#   ./scripts/profilerApp exhaustive_profiling $i $i 1000000 100 ${PLATFORM} |& tee logs/profiling/${NAME}_${i}-${i}_${PLATFORM}_SGDoperators+medHighcard.log
#   ./scripts/restart.sh spark ${PLATFORM} all ${NAME}
#   ./scripts/profilerApp exhaustive_profiling $i $i 5000000 100 ${PLATFORM} |& tee logs/profiling/${NAME}_${i}-${i}_${PLATFORM}_SGDoperators+highcard.log
#   ./scripts/restart.sh spark ${PLATFORM} all ${NAME}
#   ./scripts/profilerApp exhaustive_profiling $i $i 1000000 1000 ${PLATFORM} |& tee logs/profiling/${NAME}_${i}-${i}_${PLATFORM}_SGDoperators+highcard.log
#   echo "SubIteration $i executed"
#done
#done
