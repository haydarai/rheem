#!/usr/bin/env bash

NAME="generator"
CLASS="org.qcri.rheem.apps.wordcount.Generator"

. ../base/base.sh


. ../base/execute.sh hdfs://${IP}:8300/data/dummy/KB_100 10 10 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/KB_500 50 10 1024 |& tee generator.log

. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_1 1024 1 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_5 5120 1 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_10 10240 1 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_20 20480 1 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_50 512 100 1024 |& tee generator.log

. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_100 1024 100 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_200 2048 100 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_500 5120 100 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/MB_800 8192 100 1024 |& tee generator.log

. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_1 1024 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_2 2048 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_4 4096 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_8 8192 1024 1024 |& tee generator.log


. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_10 10240 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_15 15360 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_20 20480 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_40 40960 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_80 81920 1024 1024 |& tee generator.log


. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_100 102400 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_200 204800 1024 1024 |& tee generator.log
. ../base/execute.sh hdfs://${IP}:8300/data/dummy/GB_500 512000 1024 1024 |& tee generator.log
#. ../base/execute.sh hdfs://${IP}:8300/data/dummy/TB_1 1048576 1024 1024 |& tee generator.log


hdfs -mv /data/dummy/KB_100 /data/dummy/syntetic-1024-100
hdfs -mv /data/dummy/KB_500 /data/dummy/syntetic-1024-500
hdfs -mv /data/dummy/MB_1 /data/dummy/syntetic-1024-1024
hdfs -mv /data/dummy/MB_5 /data/dummy/syntetic-1024-5120
hdfs -mv /data/dummy/MB_10 /data/dummy/syntetic-1024-10240
hdfs -mv /data/dummy/MB_20 /data/dummy/syntetic-1024-20480
hdfs -mv /data/dummy/MB_50 /data/dummy/syntetic-1024-51200
hdfs -mv /data/dummy/MB_100 /data/dummy/syntetic-1024-102400
hdfs -mv /data/dummy/MB_200 /data/dummy/syntetic-1024-204800
hdfs -mv /data/dummy/MB_500 /data/dummy/syntetic-1024-512000
hdfs -mv /data/dummy/MB_800 /data/dummy/syntetic-1024-819200
hdfs -mv /data/dummy/GB_1 /data/dummy/syntetic-1024-1048576
hdfs -mv /data/dummy/GB_2 /data/dummy/syntetic-1024-2097152
hdfs -mv /data/dummy/GB_4 /data/dummy/syntetic-1024-4194304
hdfs -mv /data/dummy/GB_8 /data/dummy/syntetic-1024-8388608
hdfs -mv /data/dummy/GB_10 /data/dummy/syntetic-1024-10485760
hdfs -mv /data/dummy/GB_15 /data/dummy/syntetic-1024-15728640
hdfs -mv /data/dummy/GB_20 /data/dummy/syntetic-1024-20971520
hdfs -mv /data/dummy/GB_40 /data/dummy/syntetic-1024-41943040
hdfs -mv /data/dummy/GB_80 /data/dummy/syntetic-1024-83886080
hdfs -mv /data/dummy/GB_100 /data/dummy/syntetic-1024-104857600
hdfs -mv /data/dummy/GB_200 /data/dummy/syntetic-1024-209715200
hdfs -mv /data/dummy/GB_500 /data/dummy/syntetic-1024-524288000
hdfs -mv /data/dummy/TB_1 /data/dummy/syntetic-1024-1073741824































