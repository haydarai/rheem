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

#if not define SPARK_HOME
if [ -z "${SPARK_HOME}" ]; then
	echo "the SPARK_HOME are not defined"
	exit 1
fi

#if not define FLINK_HOME
if [ -z "${FLINK_HOME}" ]; then
	echo "the FLINK_HOME are not defined"
	exit 1
fi

#if not define HADOOP_HOME
if [ -z "${HADOOP_HOME}" ]; then
	echo "the HADOOP_HOME are not defined"
	exit 1
fi

#TODO: add alluxio

# Bootstrap the classpath.
RHEEM_CLASSPATH="${BASEDIR}/${FOLDER_CONF}:${BASEDIR}/${FOLDER_CODE}/*:${BASEDIR}/${FOLDER_LIBS}/*"

RHEEM_CLASSPATH="${RHEEM_CLASSPATH}:${SPARK_HOME}/jars/*"

RHEEM_CLASSPATH="${RHEEM_CLASSPATH}:${FLINK_HOME}/lib/*"

RHEEM_CLASSPATH="${RHEEM_CLASSPATH}:${HADOOP_HOME}/hadoop/*:${HADOOP_HOME}/hadoop-hdfs/*"

HOSTNAME=""

FLAGS=""
if [ "${FLAG_AKKA}" = "true" ]; then
    FLAGS="${FLAGS} -Dakka.remote.netty.tcp.hostname=${IP} -Dakka.remote.netty.tcp.maximum-frame-size=1347375965b"
fi

if [ "${FLAG_LOG}" = "true" ]; then
    FLAGS="${FLAGS} -Dlog4j.configuration=file://${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_LOG}"
fi

if [ "${FLAG_RHEEM}" = "true" ]; then
    getFromTemplate ${BASEDIR}/${FOLDER_CONF}/template/${NAME_CONF_RHEEM} ${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_RHEEM}
    FLAGS="${FLAGS} -Drheem.configuration=file://${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_RHEEM}"
fi

if [ "${FLAG_FLINK}" = "true" ]; then
    getFromTemplate ${BASEDIR}/${FOLDER_CONF}/template/${NAME_CONF_FLINK} ${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_FLINK}
    FLAGS="${FLAGS} -Dflink.configuration=file://${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_FLINK}"
fi

if [ "${FLAG_SPARK}" = "true" ]; then
    getFromTemplate ${BASEDIR}/${FOLDER_CONF}/template/${NAME_CONF_SPARK} ${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_SPARK}
    FLAGS="${FLAGS} -Dspark.configuration=file://${BASEDIR}/${FOLDER_CONF}/${NAME_CONF_SPARK}"
fi

if [ -n "${OTHER_FLAGS}" ]; then
    FLAGS="${FLAGS} ${OTHER_FLAGS}"
fi

CONTEXT="true"