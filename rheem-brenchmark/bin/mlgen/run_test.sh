#!/usr/bin/env bash



#PLATFORMS=("spark" "java" "flink" "java,spark" "java,flink" "java,spark,flink")
#PLATFORMS=("java")
PLATFORMS=("spark")
i="4"
#. ./../base/restart.sh all
for PLATFORM in ${PLATFORMS[@]}
do
        OTHER_FLAGS=" -Dspark.rpc.message.maxSize=500"
        . ./base_mlgen.sh $PLATFORM ${i} ${i} "100,500,1024,5120,10240,20480,51200,102400,204800,512000,819200,1048576,2097152,4194304,8388608,10485760,15728640,20971520,41943040,83886080,104857600,209715200,524288000,1073741824" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "100,500,1024,5120,10240,20480,51200,102400,204800,512000,819200,1048576,2097152" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "4194304" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "8388608" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "10485760" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "15728640" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "20971520" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "41943040" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "83886080" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "104857600" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "209715200" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "524288000" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"
      #  . ./base_mlgen.sh $PLATFORM ${i} ${i} "1073741824" "1024" "exhaustive_profiling" "single_spark" "operators+highcard" "false"

done