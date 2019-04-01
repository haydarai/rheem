#!/usr/bin/env bash


#PLATFORMS=("spark" "java" "flink" "java,spark" "java,flink" "java,spark,flink")
PLATFORMS=("flink,java")

#. ./../base/restart.sh all
for PLATFORM in ${PLATFORMS[@]}
do
    for i in {1..9}
    do
        . ./base_mlgen.sh $PLATFORM ${i} ${i} "1,100,1000,10000,100000" "1,10,100,1000" "exhaustive_profiling" "multiplatform_flink_java" "operators+lowcard.log" "true"

        . ./base_mlgen.sh $PLATFORM ${i} ${i} "1000000" "1,10,100" "exhaustive_profiling" "multiplatform_flink_java" "operators+lowMedcard" "true"

        . ./base_mlgen.sh $PLATFORM ${i} ${i} "1000000" "100" "exhaustive_profiling" "multiplatform_flink_java" "operators+medHighcard" "true"

        . ./base_mlgen.sh $PLATFORM ${i} ${i} "5000000" "100" "exhaustive_profiling" "multiplatform_flink_java" "operators+highcard" "true"

        . ./base_mlgen.sh $PLATFORM ${i} ${i} "1000000" "1000" "exhaustive_profiling" "multiplatform_flink_java" "operators+highcard" "true"
    done
done