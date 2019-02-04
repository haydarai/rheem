#!/usr/bin/env bash


platforms=("flink" "spark")

sizes=("0001" "0002" "0003" "0005")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        restart.sh java,flink,spark all &> /dev/null
        echo "./word2nvec.sh ${plat} ${si}"
        word2nvec.sh ${plat} ${si}
    done
done