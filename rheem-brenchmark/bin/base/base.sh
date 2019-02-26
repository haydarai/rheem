#!/usr/bin/env bash

if [ -z "${BASEDIR}" ]; then
    BASEDIR="/disk/ml-experiments"
fi

if [ -z "${FOLDER_CODE}" ]; then
    FOLDER_CODE="code"
fi

if [ -z "${FOLDER_CONF}" ]; then
    FOLDER_CONF="conf"
fi

if [ -z "${FOLDER_LIBS}" ]; then
    FOLDER_LIBS="libs"
fi

if [ -z "${FLAG_AKKA}" ]; then
    FLAG_AKKA="true"
fi

if [ -z "${FLAG_LOG}" ]; then
    FLAG_LOG="true"
fi

if [ -z "${NAME_CONF_LOG}" ]; then
    NAME_CONF_LOG="log4j.properties"
fi

if [ -z "${FLAG_RHEEM}" ]; then
    FLAG_RHEEM="true"
fi

if [ -z "${NAME_CONF_RHEEM}" ]; then
    NAME_CONF_RHEEM="rheem.properties"
fi

if [ -z "${FLAG_FLINK}" ]; then
    FLAG_FLINK="false"
fi

if [ -z "${NAME_CONF_FLINK}" ]; then
    NAME_CONF_FLINK="flink.properties"
fi

if [ -z "${FLAG_SPARK}" ]; then
    FLAG_SPARK="false"
fi

if [ -z "${NAME_CONF_SPARK}" ]; then
    NAME_CONF_SPARK="spark.properties"
fi

if [ -z "${OTHER_FLAGS}" ]; then
    OTHER_FLAGS="-Xms20g -Xmx10g -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:ParallelGCThreads=4"
fi
. $(pwd)/../base/set_variables.sh

#COMMAND TO USE IN THE SCRIPT
if [ -z "${HDFS}" ]; then
    HDFS="${HADOOP_HOME}/bin/hdfs dfs"

    HDFS_MKDIR="${HDFS} -mkdir -p "
    HDFS_DELETE="${HDFS} -rm -r -skipTrash "

    HDFS_UPLOAD="${HDFS} -copyFromLocal "
    HDFS_DOWNLOAD="${HDFS} -getmerge "

    HDFS_COPY="${HDFS} -cp "
    HDFS_MOVE="${HDFS} -mv "

    HDFS_LIST="${HDFS} -ls "
fi

FOLDER=$(pwd)/../logs/${NAME}/

if [ ! -d "${FOLDER}" ]; then
    mkdir -p ${FOLDER}
fi

. $(pwd)/../base/enviroment.sh
