#!/bin/bash

. ./conf.sh

day=`date -d yesterday +%Y-%m-%d`
date=`date -d yesterday +%Y%m%d`

input_path=$HDFS_INPUT_ROOT/raw/access_log.$day*
output_path=$HDFS_INPUT_ROOT/$date

$HADOOP_BIN fs -rm -r $output_path

$HADOOP_BIN jar \
    ../lib/tracker-hive-jar-with-dependencies.jar \
    com.tracker.hive.mapred.ApacheLogCleaned \
    $input_path \
    $output_path

ret=`echo $?`
if [ $ret -eq 0 ]; then
    echo "ApacheLogCleaned success 清洗成功!!"
else
    echo $ret + "=========   other exception in com.tracker.hive.ApacheLogCleaned ===="
    echo "ApacheLog清洗数据异常"|mutt -s "清洗数据异常 -- com.tracker.hive.ApacheLogCleaned" xiaorui.lu@51job.com
    exit 2
fi

#nohup sh web.sh > "../log/run_web.log."$date 2>&1 &

nohup sh search.sh > "../log/run_search.log."$date 2>&1 &
