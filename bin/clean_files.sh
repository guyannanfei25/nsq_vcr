#! /bin/bash

for i in `seq 1 4`; do
    find /data${i}/nsq_backup/ -type f -mtime +2 -delete
done
