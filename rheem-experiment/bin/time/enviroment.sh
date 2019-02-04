#!/usr/bin/env bash

#the class for execution is not define is imposible execute the script
if [ -z "${CLASS}" ]; then
        echo "the class is not define"
        exit 1
fi

#the folder for execute
if [ -z "${FOLDER_CODE}" ]; then
        echo "the folder of the code is not define"
        exit 1
fi

#the folder for configuration
if [ -z "${FOLDER_CONF}" ]; then
        echo "the folder of the configuration is not define"
        exit 1
fi

#the folder for libreries
if [ -z "${FOLDER_LIBS}" ]; then
        echo "the folder of the libraries is not define"
        exit 1
fi


#if not define BASEDIR
if [ -z "${BASEDIR}" ]; then
	echo "the base dir are not defined"
	exit 1
fi

# Bootstrap the classpath.
RHEEM_CLASSPATH="${BASEDIR}/${FOLDER_CONF}:${BASEDIR}/${FOLDER_CODE}/*:${BASEDIR}/${FOLDER_LIBS}/*"

#if nof define SPARK_HOME
if [ -z "${SPARK_HOME}" ]; then
      SPARK_HOME=/data/platform/code/processing/spark/spark-2.4.0-bin-hadoop2.6
fi
RHEEM_CLASSPATH="${RHEEM_CLASSPATH}:${SPARK_HOME}/jars/*"

#if not define HADOOP_HOME
if [ -z "${HADOOP_HOME}" ]; then
    HADOOP_HOME=/data/platform/code/storage/hadoop/hadoop-2.6.5
fi
RHEEM_CLASSPATH="${RHEEM_CLASSPATH}:${HADOOP_HOME}/hadoop/*:${HADOOP_HOME}/hadoop-hdfs/*"

#if not define FLINK_HOME
if [ -z "${FLINK_HOME}" ]; then
    FLINK_HOME=/data/platform/code/processing/flink/flink-1.7.1
fi
RHEEM_CLASSPATH="${RHEEM_CLASSPATH}:${FLINK_HOME}/lib/*"

HOSTNAME=""

FLAGS=""
if [ "${FLAG_AKKA}" = "true" ]; then
    FLAGS="${FLAGS} -Dakka.remote.netty.tcp.hostname=10.4.4.32 -Dakka.remote.netty.tcp.maximum-frame-size=1347375965b"
fi

if [ "${FLAG_LOG}" = "true" ]; then
    FLAGS="${FLAGS} -Dlog4j.configuration=file://${BASEDIR}/${FOLDER_CONF}/log4j.properties"
fi

if [ "${FLAG_RHEEM}" = "true" ]; then
    FLAGS="${FLAGS} -Drheem.configuration=file://${BASEDIR}/${FOLDER_CONF}/rheem.properties"
fi

if [ "${FLAG_FLINK}" = "true" ]; then
    FLAGS="${FLAGS} -Dflink.configuration=file://${BASEDIR}/${FOLDER_CONF}/flink.properties"
fi

if [ "${FLAG_SPARK}" = "true" ]; then
    FLAGS="${FLAGS} -Dspark.configuration=file://${BASEDIR}/${FOLDER_CONF}/spark.properties"
fi

if [ -n "${OTHER_FLAGS}" ]; then
    FLAGS="${FLAGS} ${OTHER_FLAGS}"
fi

CONTEXT="true"