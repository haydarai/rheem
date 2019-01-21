#!/usr/bin/env bash

#!/usr/bin/env bash

list_files=$(cat ./files_splited.txt)

for file in ${list_files}; do
    cat ${file}.part* > ${file}
    rm -rf ${file}.part*
done

