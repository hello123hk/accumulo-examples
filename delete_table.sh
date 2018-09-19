#!/usr/bin/env bash

echo yes| accumulo shell -u root -p yidu -e "deletetable batch"
accumulo shell -u root -p yidu -e "createtable batch"
accumulo shell -u root -p yidu -e "addsplits -t batch 1 3 5 7 9 0 a e j m o t u z"


if [ $# -ne 0 ]; then
    echo "wal is false"
    accumulo shell -u root -p yidu -e "config -t batch -s table.walog.enabled=false"
fi


