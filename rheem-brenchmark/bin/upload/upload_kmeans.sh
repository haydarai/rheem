#!/usr/bin/env bash

SIZES=("0010" "0100" "1000")
#SIZES=("0001")

for size in ${SIZES[@]}; do
    . ./base_upload.sh "census" "" ${size} "10"
done