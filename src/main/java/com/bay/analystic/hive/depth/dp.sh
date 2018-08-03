#!/bin/bash
 
#./en.sh -d 2018-08-01
 
#获取输入的日期参数
run_date=
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
then
shift
run_date=$1
fi
shift
done
 
#如果获取不到则自动设置为系统时间的前一天
if [ -n $run_date ]
then
echo "$run_date"
else
run_date=`date -d "1 day ago" "+%y-%m-%d"`
fi
 
month=`date -d "$run_date" "+%m"`
day=`date -d "$run_date" "+%d"`
 
echo "final running date is:${run_date} ,month is:${month} ,day is ${day}"
 
echo "
###############################################
##############      RUN HQL      ##############
###############################################
"
 
#执行HQL语句
hive -f /home/logAnalystic/depth/dp.sql -hiveconf month=${month} -hiveconf day=${day} >> /home/logAnalystic/depth/result.txt

echo "
###############################################
##############     RUN SQOOP     ##############
###############################################
"
 
#执行sqoop
sqoop export --connect jdbc:mysql://hadoop010:3306/result --username root --password 19950116 --table stats_view_depth --export-dir hdfs://hadoop010:9000/user/hive/warehouse/loganalystic.db/stats_view_depth --input-fields-terminated-by '\001' --update-mode allowinsert
 
echo "OK!!"
