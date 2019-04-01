#!/usr/bin/env bash

SIZES=("0001" "0005" "0010" "0025" "0050" "0100")

for size in ${SIZES[@]}; do
    . ./base_upload.sh "pages_unredirected" "" ${size} "10"
done