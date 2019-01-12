#!/usr/bin/env bash

platforms=("flink" "spark")

sizes=("0010" "0025" "0050" "0100" "0200")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./sgd.sh ${plat} ${si}"
        ./sgd.sh ${plat} ${si}
    done
done