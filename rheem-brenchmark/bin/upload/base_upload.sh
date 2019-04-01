#!/usr/bin/env bash


. ./../base/set_variables.sh

#FOLDER THAT HAVE THE DATA
NAME=$1
EXTENCION=$2
SIZE=$3
PARTITION_SIZE=$4

#FOLDER IN LOCAL THAT CONTAINS THE DATA
FL_BASE=/export/da-cluster-data/dataset/${NAME}/${NAME}_${SIZE}_${PARTITION_SIZE}.part

#FOLDER IN HDFS THAT WILL CONTAINS THE DATA
FH_INPUT=/data/${NAME}/${SIZE}

#SEQUENCI OF THE DATA TO WORK
SEQ=$(seq -f "%02g" 0 $(($PARTITION_SIZE - 1)))

IPS=($(get_list_cluster))

pos=-1
for i in ${SEQ} ; do
#SELECTING THE CURRENT IP START
    pos=$(($pos + 1))
    ip=${IPS[$(($pos%$PARTITION_SIZE))]}
    echo "Uplod files in the ip: 10.4.4.$ip"
#SELECTING THE CURRENT IP FINISH

#BUILD THE STRUCTURE OF FOLDER START
    #FOLDER OF DATA
    echo "creating the folder in HDFS for the file ${FH_INPUT} ${i}"
    ${HDFS_MKDIR} ${FH_INPUT}
#BUILD THE STRUCTURE OF FOLDER FINISH

#UPLOADING THE FILES IN HDFS START
    echo "uploading the file ${FOL_WORDS} in HDFS for the size ${SIZE} in the ip 10.4.4.${ip}"
    ssh 10.4.4.${ip} "${HDFS_UPLOAD} ${FL_BASE}/${NAME}_${SIZE}${EXTENCION}.part${i} ${FH_INPUT}/"
#UPLOADING THE FILES IN HDFS FINISH
done



