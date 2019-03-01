#!/usr/bin/env bash

FILE_TO_PARTICION=$1
NUMBER_OF_PARTICION=$2
FILES_OUTPUT=$3


size=$(wc -l <${FILE_TO_PARTICION})
parts=$((${size}/${NUMBER_OF_PARTICION}))

split -l ${parts} -d ${FILE_TO_PARTICION} "${FILE_TO_PARTICION}.part"

if [[ ! -d ${FILES_OUTPUT} ]]
then
  mkdir -p $(FILES_OUTPUT)
fi
mv ${FILE_TO_PARTICION}.part* ${FILES_OUTPUT}