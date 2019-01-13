#!/usr/bin/env bash

platforms=("flink" "spark")

sizes=("0001" "0005" "0010" "0025" "0050" "0100")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./crocopr.sh ${plat} ${si}"
        ./crocopr.sh ${plat} ${si}
    done
done