#!/usr/bin/env bash

if [ -z "${BASE_PATH}" ]; then
    BASE_PATH=/plat
fi

if [ -z "${T_PROCESSING}" ]; then
    T_PROCESSING="processing"
fi

if [ -z "${T_STORAGE}" ]; then
    T_STORAGE="storage"
fi

if [ -z "${VERSION_RHEEM}" ]; then
    VERSION_RHEEM="0.5.0"
fi

if [ -z "${VERSION_SPARK}" ]; then
    VERSION_SPARK="2.4.0_hadoop2.6"
    #VERSION_SPARK="1.6.3_hadooop2.6"
fi

if [ -z "${VERSION_HDFS}" ]; then
    VERSION_HDFS="2.6.5"
fi

if [ -z "${VERSION_ALLUXIO}" ]; then
    VERSION_ALLUXIO="1.8.1"
fi

if [ -z "${VERSION_FLINK}" ]; then
    VERSION_FLINK="1.7.1"
fi

#if nof define SPARK_HOME
if [ -z "${SPARK_HOME}" ]; then
      SPARK_HOME=${BASE_PATH}/${T_PROCESSING}/spark/${VERSION_SPARK}
fi

#if not define FLINK_HOME
if [ -z "${FLINK_HOME}" ]; then
    FLINK_HOME=${BASE_PATH}/${T_PROCESSING}/flink/${VERSION_FLINK}
fi

#if not define HADOOP_HOME
if [ -z "${HADOOP_HOME}" ]; then
    HADOOP_HOME=${BASE_PATH}/${T_STORAGE}/hdfs/${VERSION_HDFS}
fi

#if not define ALLUXIO_HOME
if [ -z "${ALLUXIO_HOME}" ]; then
    ALLUXIO_HOME=${BASE_PATH}/${T_STORAGE}/alluxio/${VERSION_ALLUXIO}
fi

