#!/usr/bin/env bash

function get_list_cluster() {
    my_ip=$(hostname --ip-address)
    ips=()
    if [ "${my_ip}" = "10.4.4.32" ]; then
        ips=("32" "35" "33" "25" "36" "23" "34" "29" "28" "24")
    elif [ "${my_ip}" = "10.4.4.30" ]; then
        ips=("30" "31" "22" "26" "27" "48" "70" "46" "41" "37")
    elif [ "${my_ip}" = "10.4.4.43" ]; then
        ips=("43" "47" "38" "42" "54" "40" "45" "65" "39" "44")
    else
        echo "the ${my_ip} not correspond to a valid ip, please check the ip of the machine or check that you are runnig the code in the right machine"
        exit 1
    fi

    for ip in ${ips[@]}; do
        echo "${ip}"
    done

}