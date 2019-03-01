#!/usr/bin/env bash

TIMEOUT=7200
sizes=( "0-1" "0001" "0002" "0003" "0005" "0100")
platforms=("spark" "flink" "java")
restart="true"
for plat in ${platforms[@]}; do
    for size in ${sizes[@]}; do
        for iter in 1 2 3 ; do
            . ./base_simwords.sh ${plat} ${size} ${iter} ${restart}
        done
    done
done