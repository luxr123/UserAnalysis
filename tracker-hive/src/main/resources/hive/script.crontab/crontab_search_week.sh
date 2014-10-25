#!/bin/bash
. ./conf.sh

if [ $# -eq 1 ];
then
	week=$1
else
    week=`date +%Y%m%V -d '7 days ago'`
fi

nohup $HIVE_BIN -hiveconf week=$week -f week_search_stats.sql > ../log/search-week-$week.log 2>&1 &
