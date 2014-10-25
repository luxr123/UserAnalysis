if [ $# -eq 1 ];
then
	date=$1
else
	date=`date -d yesterday +%Y%m%d`
fi

nohup sh run.sh > "../log/run.log."$date 2>&1 &
