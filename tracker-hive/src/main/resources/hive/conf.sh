#!/bin/bash
ROOT=/home/hadoop/lu/kpi
CONF=$ROOT/conf
LOG=$ROOT/log
LIB=$ROOT/lib
DATA=$ROOT/data

HADOOP_BIN=/home/hadoop/secondary/hadoop-2.2.0/bin/hadoop
HIVE_BIN=/home/hadoop/secondary/apache-hive-0.13.0-bin/bin/hive

HDFS_INPUT_ROOT=/apache_log
HDFS_WEB_OUTPUT_ROOT=/cleaned_web_data
HDFS_SEARCH_OUTPUT_ROOT=/cleaned_search_data

APACHEPVLOG=apache_pv_log
APACHESEARCHLOG=apache_search_log
APACHEWEB=apache_web
APACHESEARCH=apache_search
MANAGERSEARCH=manager_search
CASESEARCH=case_search
