#!/usr/bin/env bash

. ./../base/set_variables.sh


index=$1
start=$(($index*10 - 9))
finish=$(($start + 9))

#factor1=("0.01" "0.02" "0.03" "0.05" "0.1" "0.15" "0.25" "0.50" "1" "2" "5" "10")
factor1=()
#factor2=("100" "200" "500" "1000")
factor2=("200")

tables_name=("customer" "supplier" "nation" "region" "orders" "lineitem" "part" "partsupp")
#tables_name=("customer" "orders" "lineitem")

for factor in ${factor1[@]}
do
    for table in ${!tables_name[*]}
    do
        echo "uploading ${tables_name[$table]}/${factor} particion $index"
        FOLDER=/export/da-cluster-data/dataset/tpch/${tables_name[$table]}/${factor}
        ${HDFS_MKDIR} /data/tpch/${tables_name[$table]}/${factor}
        if [ ${tables_name[$table]} = "nation" ]; then
            ${HDFS_UPLOAD} \
              ${FOLDER}/${tables_name[$table]}.tbl \
              /data/tpch/${tables_name[$table]}/${factor}/part-0
        else
            if [ ${tables_name[$table]} = "region" ]; then
               ${HDFS_UPLOAD} \
                  ${FOLDER}/${tables_name[$table]}.tbl \
                  /data/tpch/${tables_name[$table]}/${factor}/part-0
            else
                 ${HDFS_UPLOAD} \
                    ${FOLDER}/${tables_name[$table]}.tbl.${index} \
                    /data/tpch/${tables_name[$table]}/${factor}/part-${index}
            fi
        fi
    done
done


for factor in ${factor2[@]}
do
    for table in ${!tables_name[*]}
    do
        for index in $(seq $start $finish)
        do
            echo "uploading ${tables_name[$table]}/${factor} particion $index"
            FOLDER=/export/da-cluster-data/dataset/tpch/${tables_name[$table]}/${factor}
            ${HDFS_MKDIR} /data/tpch/${tables_name[$table]}/${factor}
            if [ ${tables_name[$table]} = "nation" ]; then
                ${HDFS_UPLOAD} \
                  ${FOLDER}/${tables_name[$table]}.tbl \
                  /data/tpch/${tables_name[$table]}/${factor}/part-0
            else
                if [ ${tables_name[$table]} = "region" ]; then
                    ${HDFS_UPLOAD} \
                      ${FOLDER}/${tables_name[$table]}.tbl \
                      /data/tpch/${tables_name[$table]}/${factor}/part-0
                else
                   ${HDFS_UPLOAD} \
                    ${FOLDER}/${tables_name[$table]}.tbl.${index} \
                    /data/tpch/${tables_name[$table]}/${factor}/part-${index}
               fi
            fi
         done
    done
done