#!/usr/bin/env bash

TIMEOUT=1800
sizes=( "0010" "0025" "0050" "0100" "0200" "0400")
platforms=("spark" "flink" "java")
restart="true"
for plat in ${platforms[@]}; do
    for size in ${sizes[@]}; do
        for iter in 1 2 3 ; do
            . ./base_sgd.sh ${plat} ${size} ${iter} ${restart}
        done
    done
done