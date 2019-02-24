#!/usr/bin/env bash

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_aggregate.sh spark "0200" 1

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_aggregate.sh spark "0200" 2

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_aggregate.sh spark "0200" 3

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_aggregate.sh flink "0200" 1

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_aggregate.sh flink "0200" 2

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_aggregate.sh flink "0200" 3


. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_sgd.sh flink "0100" 1

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_sgd.sh flink "0100" 2

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_sgd.sh flink "0100" 3