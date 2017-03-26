#! /bin/bash

for i in `seq 1 4`; do
    if [ ! -d /data${i}/nsq_backup ]; then
        echo "/data${i}/nsq_backup is not dir, please recheck!!!"
        echo "Install Failed!!!!!"
        exit -1
    fi
done

if [ ! -d /home/s/data/nsq_vcr ]; then
    mkdir -p /home/s/data/nsq_vcr
    chown cloud:cloud -R /home/s/data/nsq_vcr
fi

rm -fv /home/s/nsq_vcr
cur_path=`dirname $0`
cur_path=`cd ${cur_path};pwd`
cur_path=`dirname ${cur_path}`

cd /home/s
ln -sf ${cur_path} nsq_vcr
chown cloud:cloud -R ${cur_path}
chown cloud:cloud -R nsq_vcr

echo "Install success. Please cp cron due to your business type."
