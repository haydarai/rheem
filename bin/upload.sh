#!/usr/bin/env bash


#mvn -DskipTests=true package -P distro


CP_REMOTE="sshpass -p Qatar1234! scp "
EXEC_REMOTE="sshpass -p Qatar1234! ssh QCRI\\brojas@10.4.4.32 "


#${EXEC_REMOTE} 'rm -rf /disk/experiments/bin/* /disk/experiments/code/* /disk/experiments/conf/*'
${EXEC_REMOTE} 'rm -rf /disk/experiments/code/* '


${CP_REMOTE} -r /Users/notjarvis/IdeaProjects/rheem-experiments/rheem-distro/target/rheem-distro_2.11-0.5.0-SNAPSHOT-distro/rheem-distro_2.11-0.5.0-SNAPSHOT/* \
    QCRI\\brojas@10.4.4.32:/disk/experiments/code

${EXEC_REMOTE} 'rm -rf /disk/experiments/code/netty*'


${CP_REMOTE} -r /Users/notjarvis/IdeaProjects/rheem-experiments/rheem-experiment/bin/* \
    QCRI\\brojas@10.4.4.32:/disk/experiments/bin


${CP_REMOTE} -r /Users/notjarvis/IdeaProjects/rheem-experiments/rheem-experiment/conf/* \
    QCRI\\brojas@10.4.4.32:/disk/experiments/conf