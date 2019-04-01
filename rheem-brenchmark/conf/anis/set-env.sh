#!/bin/bash

#SPARK_HOME=/root/zoi/spark-1.6.2-bin-without-hadoop
#SPARK_HOME=/opt/spark-1.6.0-bin-hadoop2.6/spark-1.6.0-bin-hadoop2.6
SPARK_HOME=/opt/spark-1.6.0-compiled-hadoop2.6-with-ganglia
#SPARK_HOME=/root/spark-1.6.0
#SPARK_HOME=/root/spark-2.1.0-bin-hadoop2.6
# Only specify, if your Spark version comes without Hadoop.
#HADOOP_HOME=/root/zoi/hadoop-2.6.2
HADOOP_HOME=/opt/cloudera/parcels/CDH-5.9.1-1.cdh5.9.1.p0.4/lib/hadoop

FLINK_HOME=/usr/etc/flink-1.3.2_2.10/lib

basedir=/root/anis

# Bootstrap the classpath.
#basedir=$(readlink -m "$0/../..")
#RHEEM_CLASSPATH="$basedir/conf:$basedir/rheem-distro-0.2.2-SNAPSHOT-profiling-with-flink/*"
#RHEEM_CLASSPATH="$basedir/conf:$basedir/rheem-distro-0.2.2-SNAPSHOT-profiling-with-flink-mloptimizer/*"
#RHEEM_CLASSPATH="$basedir/conf:$basedir/rheem-distro-0.2.2-SNAPSHOT-scala-2.10/*"
#RHEEM_CLASSPATH="$basedir/conf:$basedir/rheem-distro-0.2.2-SNAPSHOT-ecosystem/*"
#RHEEM_CLASSPATH="$RHEEM_CLASSPATH:$basedir/tmp3/*"
RHEEM_CLASSPATH="$basedir/conf:$basedir/rheem-distro_2.10-0.3.1-SNAPSHOT/*"

if [ "$SPARK_HOME" != "" ]; then
  #RHEEM_CLASSPATH="$RHEEM_CLASSPATH:$SPARK_HOME/lib/*"
  RHEEM_CLASSPATH="$RHEEM_CLASSPATH:$SPARK_HOME/lib/*"
fi

if [ "$HADOOP_HOME" != "" ]; then
  RHEEM_CLASSPATH="$RHEEM_CLASSPATH:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*"
fi

if [ "$FLINK_HOME" != "" ]; then
  RHEEM_CLASSPATH="$RHEEM_CLASSPATH:$FLINK_HOME/*"
fi
