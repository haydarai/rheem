#!/usr/bin/env bash


TIMEOUT=3600
sizes=( "0001" "0010" "0025" "0050" "0100" "0200" "1000")
platforms=("flink" "java")
restart="true"
for plat in ${platforms[@]}; do
    for size in ${sizes[@]}; do
        for iter in 1 2 3 ; do
            . ./base_wordcount.sh ${plat} ${size} ${iter} ${restart}
        done
    done
done