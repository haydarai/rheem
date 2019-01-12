#!/usr/bin/env bash


platforms=("flink" "spark")

sizes=("1" "10" "100" "200")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./tpch-q3.sh ${plat} ${si}"
        ./tpch-q3.sh ${plat} ${si}
    done
done