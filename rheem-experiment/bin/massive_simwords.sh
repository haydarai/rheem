#!/usr/bin/env bash


platforms=("flink")

sizes=("0001" "0002" "0003" "0005")

for plat in ${platforms[@]}
do
    for si in ${sizes[@]}
    do
        ./restart.sh java,flink,spark all &> /dev/null
        echo "./simwords.sh ${plat} ${si}"
        ./simwords.sh ${plat} ${si}
    done
done