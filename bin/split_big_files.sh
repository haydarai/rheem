#!/usr/bin/env bash

list_files=$(find ./.. -type f -size +100M | grep -v .git)
DESTINATION=files_splited.txt

touch ${DESTINATION}
git add ${DESTINATION}
for file in ${list_files}; do
    echo ${file} >> ${DESTINATION}
    split -b 52428800 -d ${file} "${file}.part"
    git add ${file}.part*
    rm -rf ${file}
    git rm ${file}
done