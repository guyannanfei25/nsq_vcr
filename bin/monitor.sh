#! /bin/bash
if [ $# -ne 1 ]; then
    echo "usage: ${0} type[record|play]"
    exit 0
fi

nowTime=`date "+%Y-%m-%d %H:%M:%S"`

pid=`cat /tmp/data/nsq_vcr/${1}.pid`
ps axu|grep ${pid}|grep -v grep
if [ $? -ne 0 ]; then
    # restart
    echo "${nowTime} ${1} process not alive now try to restart" >> /tmp/data/nsq_vcr/restart.log
    nohup /tmp/nsq_vcr/bin/${1} -f /tmp/nsq_vcr/etc/${1}.json >> /tmp/data/nsq_vcr/${1}_access.log 2>&1 &
else
    echo "${nowTime} ${1} process work well" >> /tmp/data/nsq_vcr/restart.log
fi
