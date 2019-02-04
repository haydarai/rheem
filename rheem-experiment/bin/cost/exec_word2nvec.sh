#!/usr/bin/env bash

. ./../base/restart.sh all

NAME_CONF_RHEEM="rheem.properties"
. ./base_word2nvec.sh spark "0001" 1

. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_word2nvec.sh spark "0001" 2


. ./../base/restart.sh all
NAME_CONF_RHEEM="rheem.properties"
. ./base_word2nvec.sh spark "0001" 3


