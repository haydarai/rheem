#!/usr/bin/env bash

platforms=("spark" "flink")

sizes=("5000")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./wordcount.sh ${plat} ${si}"
        ./wordcount.sh ${plat} ${si}
    done
done