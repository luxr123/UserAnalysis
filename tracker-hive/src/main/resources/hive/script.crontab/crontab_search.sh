if [ $# -eq 1 ];
then
	date=$1
else
	date=`date -d yesterday +%Y%m%d`
fi

#nohup sh search.sh > "../log/run_search.log."$date 2>&1 &
#nohup sh search.sh -c > "../log/run_search.log."$date 2>&1 &
nohup sh search.sh -c -f > "../log/run_search.log."$date 2>&1 &
