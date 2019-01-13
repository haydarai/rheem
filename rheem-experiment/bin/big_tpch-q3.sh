#!/usr/bin/env bash

platforms=("spark" "flink")

sizes=("1000")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./tpch-q3.sh ${plat} ${si}"
        ./tpch-q3.sh ${plat} ${si}
    done
done