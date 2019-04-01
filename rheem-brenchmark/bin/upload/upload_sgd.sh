#!/usr/bin/env bash

SIZES=("0010" "0025" "0050" "0100" "0200" "0400")
#SIZES=("0001")

for size in ${SIZES[@]}; do
    . ./base_upload.sh "HIGGS" "" ${size} "10"
done