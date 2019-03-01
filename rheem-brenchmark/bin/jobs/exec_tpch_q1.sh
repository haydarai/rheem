#!/usr/bin/env bash

TIMEOUT=3600
sizes=( "0001" "0010" "0100" "0200")
platforms=("spark" "flink" "java")
restart="true"
for plat in ${platforms[@]}; do
    for size in ${sizes[@]}; do
        for iter in 1 2 3 ; do
            . ./base_tpch.sh ${plat} ${size} ${iter} "Q1File" ${restart}
        done
    done
done