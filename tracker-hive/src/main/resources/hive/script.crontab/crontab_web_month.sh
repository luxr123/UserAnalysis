#!/bin/bash
. ./conf.sh

if [ $# -eq 1 ];
then
	month=$1
else
    month=`date -d '1 months ago' +%Y%m`
fi

nohup $HIVE_BIN -hiveconf month=$month -f month_website_stats.sql > ../log/web-month-$month.log 2>&1 &
