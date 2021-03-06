#!/bin/bash
set -x

. ./conf.sh

logdate=`date -d yesterday +"%Y%m%d"`
year=`date -d yesterday +"%Y"`
month=`date -d yesterday +"%Y%m"`
week=`date -d yesterday +"%Y%V"`
day=`date -d yesterday +%Y%m%d`


##################################
while getopts :cf opt
do
    case $opt in
        c)
            is_cleaned=false
            ;;
        f)
            is_fact=false
            ;;
        *)
            echo "-$opt not recognized"
            ;;
    esac
done

#####################################
shift $(($OPTIND - 1))
if [ -z $1 ]; then
	input_path=,$HDFS_INPUT_ROOT/$logdate/$APACHESEARCHLOG
else
	daylist=`echo $1 | awk -F',' '{print $0}' | sed "s/,/ /g"`
	for i in $daylist
	do
		if d=`date -d $i +%g-%m-%d`; then
			input_path=$input_path","$HDFS_INPUT_ROOT/$d/$APACHESEARCHLOG
		else
			exit 2
		fi
	done
fi
input_path=`echo $input_path | sed 's/^.//'`

#####################################
# 清洗数据
if $is_cleaned; then
    $HADOOP_BIN fs -rm -R $HDFS_SEARCH_OUTPUT_ROOT/$day
    $HADOOP_BIN jar \
    ../lib/tracker-hive-jar-with-dependencies.jar \
    com.tracker.hive.mapred.SearchDataCleanedMR \
    -files file:///home/hadoop/lu/kpi/conf/qqwry.dat,file:///home/hadoop/lu/kpi/conf/universityLocation.txt \
    $input_path \
    $HDFS_SEARCH_OUTPUT_ROOT/$day

    ret=`echo $?`

    if [ $ret -eq 0 ]; then
        echo "SearchDataCleanedMR success 清洗成功!!"
    else
        echo $ret + "=========   other exception in com.tracker.hive.SearchDataCleanedMR ===="
        echo "清洗数据异常,见附件"|mutt -s "清洗数据异常 -- com.tracker.hive.SearchDataCleanedMR" xiaorui.lu@51job.com -a ../log/run_search.log.$day
        exit 2
    fi
fi

days=`$HADOOP_BIN fs -ls $HDFS_SEARCH_OUTPUT_ROOT/$day |awk -F '/' '{print $NF}' | grep -v '[Found|_SUCCESS]'`
if $is_cleaned; then
    for D in $days
    do
        Y=`date -d $D +%Y`
        M=`date -d $D +%Y%m`
        W=`date -d $D +%Y%V`
        $HIVE_BIN -e "LOAD DATA INPATH '$HDFS_SEARCH_OUTPUT_ROOT/$day/$D/$APACHESEARCH' INTO TABLE apache_search_log \
            PARTITION (year=$Y, month=$M, week=$W, day=$D);"
        $HIVE_BIN -e "LOAD DATA INPATH '$HDFS_SEARCH_OUTPUT_ROOT/$day/$D/$APACHEWEB' INTO TABLE bd_web_access_log \
            PARTITION (year=$Y, month=$M, week=$W, day=$D);"
        $HIVE_BIN -e "LOAD DATA INPATH '$HDFS_SEARCH_OUTPUT_ROOT/$day/$D/$MANAGERSEARCH' INTO TABLE manager_search_condition \
            PARTITION (year=$Y, month=$M, week=$W, day=$D);"
        $HIVE_BIN -e "LOAD DATA INPATH '$HDFS_SEARCH_OUTPUT_ROOT/$day/$D/$CASESEARCH' INTO TABLE case_search_condition \
            PARTITION (year=$Y, month=$M, week=$W, day=$D);"
    done
fi

sh crontab_web.sh

days=`echo $days | sed "s/ /,/g"`

# hive -> hive_search_fact.sql
if $is_fact; then
    $HIVE_BIN -hiveconf days="'$days'" -f hive_search_fact.sql
    ret=`echo $?`
    if [ $ret -eq 0 ]; then
        echo "hive_search_fact.sql success!!"
    else
        echo $ret + "=========   other exception in hive_search_fact.sql ===="
        echo "数据表格异常,见附件"|mutt -s "hive 数据表格异常 -- hive_search_fact.sql" xiaorui.lu@51job.com -a ../log/run_search.log.$day
        exit 2
    fi

    for i in `echo $input_path | sed "s/,/ /g"`
    do
        $HADOOP_BIN fs -mv $i $i.completed
    done
fi

# hive -> hive_search_uvip_stats.sql
$HIVE_BIN -hiveconf days="'$days'" -f hive_search_uvip_stats.sql >> ../log/hive_search.log.$day 2>&1 &

# hive -> hive_search_stats.sql
$HIVE_BIN -hiveconf days="'$days'" -f hive_search_stats.sql >> ../log/hive_search.log.$day 2>&1 &

count=0
exp=false
for pid in $(jobs -p)
do
    wait $pid
    ret=`echo $?`
    count=$(( count + 1 ))
    if [ $ret -ne 0 ]; then
        exp=true
        exp_shell=$count";"$exp_shell
    fi
done

if $exp; then
    echo "插入hbase数据异常,分别为第"$exp_shell"脚本,见附件" | mutt -s "$day: sorry, the program failed." xiaorui.lu@51job.com -a ../log/hive_search.log.$day
else
    echo "别紧张,程序正常运行完成!!(search)" | mutt -s "$day: Congratulations, the program runs normally." xiaorui.lu@51job.com
fi
