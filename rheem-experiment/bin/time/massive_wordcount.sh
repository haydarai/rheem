#!/usr/bin/env bash


platforms=("flink" "spark")

sizes=("0001" "0010" "0025" "0050" "0100" "0200" "1000")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        restart.sh java,flink,spark all &> /dev/null
        echo "./wordcount.sh ${plat} ${si}"
        wordcount.sh ${plat} ${si}
    done
done