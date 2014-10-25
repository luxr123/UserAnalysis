if [ $# -eq 1 ];
then
	date=$1
else
	date=`date -d yesterday +%Y%m%d`
fi

nohup sh web.sh > "../log/run_web.log."$date 2>&1 &
#nohup sh web.sh -c > "../log/run_web.log."$date 2>&1 &
