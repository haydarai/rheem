#!/usr/bin/env bash


source $(pwd)/../base/set_variables.sh

start_hdfs="${HADOOP_HOME}/sbin/start-dfs.sh"
stop_hdfs="${HADOOP_HOME}/sbin/stop-dfs.sh"
restart_hdfs="${stop_hdfs} && ${start_hdfs}"

start_alluxio="${ALLUXIO_HOME}/bin/alluxio-start.sh all SudoMount"
stop_alluxio="${ALLUXIO_HOME}/bin/alluxio-stop.sh all"
restart_alluxio="${stop_alluxio} && ${start_alluxio}"

start_flink="${FLINK_HOME}/bin/start-cluster.sh"
stop_flink="${FLINK_HOME}/bin/stop-cluster.sh"
restart_flink="${stop_flink} && ${start_flink}"

start_spark="${SPARK_HOME}/sbin/start-all.sh"
stop_spark="${SPARK_HOME}/sbin/stop-all.sh"
restart_spark="${stop_spark} && ${start_spark}"

stop_all="${stop_spark} ; ${stop_flink}"
start_all="${start_flink} ; ${start_spark}"

function clean_all() {
    ips=($(get_list_cluster))
    for ip in ${ips[@]}; do
        ssh 10.4.4.${ip} 'rm -rf /logs/flink/* /logs/spark/* && sync; echo 3 > /proc/sys/vm/drop_caches'
    done
}

restart_all="${stop_all} ; clean_all ;  ${start_all}"
