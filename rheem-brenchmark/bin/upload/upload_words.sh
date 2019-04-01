#!/usr/bin/env bash

#SIZES=("0001" "0002" "0003" "0005" "0010" "0025" "0050" "0100" "0200" "0800" "1000")
SIZES=("0-1")


for size in ${SIZES[@]}; do
    . ./base_upload.sh "long_abstract_clean" ".tql" ${size} "10"
done