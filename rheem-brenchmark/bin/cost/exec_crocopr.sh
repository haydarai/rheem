#!/usr/bin/env bash

#NAME_CONF_RHEEM="rheem.properties"
#. ./base_crocopr.sh spark "0010" 0
#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph" "0010" 1
#. ./base_crocopr.sh "spark,spark-graph" "0010" 1
#. ./base_crocopr.sh "flink,java-graph,java" "0010" 1
#. ./base_crocopr.sh "giraph,flink,java" "0010" 1
#. ./base_crocopr.sh "java,java-graph,flink" "0010" 1
#. ./base_crocopr.sh "flink" "0010" 1
#. ./base_crocopr.sh "spark,spark-graph" "0010" 1



#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph,flink" "0010" 1

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph,flink" "0010" 2

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph,flink" "0010" 3

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "spark,spark-graph" "0010" 1

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "spark,spark-graph" "0010" 2

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "spark,spark-graph" "0010" 3

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph" "0010" 1

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph" "0010" 2

#. ./../base/restart.sh all
#NAME_CONF_RHEEM="rheem_crocopr.properties"
#. ./base_crocopr.sh "java,java-graph" "0010" 3

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem_crocopr_spark.properties"
. ./base_crocopr.sh "java,java-graph,spark" "0010" 1

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem_crocopr_spark.properties"
. ./base_crocopr.sh "java,java-graph,spark" "0010" 2

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem_crocopr_spark.properties"
. ./base_crocopr.sh "java,java-graph,spark" "0010" 3
