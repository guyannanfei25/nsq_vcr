#! /bin/bash
if [ $# -ne 1 ]; then
    echo "usage: ${0} type[record|play]"
    exit 0
fi

kill `cat /home/s/data/nsq_vcr/${1}.pid`
