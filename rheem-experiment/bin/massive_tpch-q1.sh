#!/usr/bin/env bash


platforms=("flink" "spark")

sizes=("0001" "0010" "0100" "0200")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./tpch-qa.sh ${plat} ${si}"
        ./tpch-q1.sh ${plat} ${si}
    done
done