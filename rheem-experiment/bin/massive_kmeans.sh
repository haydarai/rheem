#!/usr/bin/env bash


platforms=("flink" "spark")

sizes=("0010" "0100" "1000")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./kmeans.sh ${plat} ${si}"
        ./kmeans.sh ${plat} ${si}
    done
done