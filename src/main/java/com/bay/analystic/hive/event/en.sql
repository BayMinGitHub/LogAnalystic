--创建数据库
create database if not exists loganalystic;
use loganalystic;

--事件分析
--创建Hive分区表映射日志数据
create external table if not exists ods_logs (
s_time string,en string,ver string,u_ud string,u_mid string,u_sid string,c_time string,language  string,b_iev string,b_rst string,p_url string,
p_ref string,tt string,pl string,o_id string,`on` string,cut string,cua string,pt string,ca string,ac string,kv_ string,du string,os string,
os_v string,browser string,browser_v string,country string,province string,city string)
partitioned by (month int, day int)
row format delimited fields terminated by '\001';
 
--加载数据(仅建表后加载一次)
load data inpath '/ods/month${hiveconf:month}/day${hiveconf:day}' into table ods_logs partition (month='${hiveconf:month}',day='${hiveconf:day}');
 
--创建Hive中的事件表(与Mysql中的对应)
create table if not exists stats_event (
`platform_dimension_id` int,
`date_dimension_id` int,
`event_dimension_id` int,
`times` int,
`created` date);
 
--创建临时表
create external table if not exists dw_event(
s_time bigint,
en string,
pl string,
ca string,
ac string
)partitioned by(month int,day int)
row format delimited fields terminated by '\001';
 
--从日志表中抽取相应数据到临时表中
insert overwrite table dw_event partition (month='${hiveconf:month}',day='${hiveconf:day}')
select s_time,en,pl,ca,ac from ods_logs where month='${hiveconf:month}' and day='${hiveconf:day}';
 
--加载自定义UDF函数
create temporary function date_convert as 'com.bay.analystic.hive.udf.DateDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';
 
create temporary function platform_convert as 'com.bay.analystic.hive.udf.PlatFormDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';
 
create temporary function event_convert as 'com.bay.analystic.hive.udf.EventDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';
 
--将处理后的数据插入最终表中
with tmp as (
select from_unixtime( cast ( de.s_time / 1000 as BIGINT), 'yyyy-MM-dd') as dt,de.pl as pl,de.ca as ca,de.ac as ac from dw_event as de
where de.pl is not null and de.month='${hiveconf:month}' and de.day='${hiveconf:day}' group by de.s_time,de.pl,de.ca,de.ac )
from (
select dt,pl as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,pl,ca,ac union all
select dt,pl as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,pl,ca union all
select dt,pl as pl,'all' as ca,'all' as ac,count(1) as ct from tmp group by dt,pl union all
select dt,'all' as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select dt,'all' as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,ca union all
select dt,'all' as pl,'all' as ca,'all' as ac,count(1) as ct from tmp group by dt
) as tmp2
insert overwrite table stats_event
select platform_convert(pl),date_convert(dt),event_convert(ca,ac),sum(ct),dt group by pl,dt,ca,ac;



----创建数据库
--create database loganalystic;
--
--use loganalystic;
--
----创建Hive分区表,加载log数据
--create external table if not exists ods_logs (
--s_time string,
--en string,
--ver string,
--u_ud string,
--u_mid string,
--u_sid string,
--c_time string,
--language  string,
--b_iev string,
--b_rst string,
--p_url string,
--p_ref string,
--tt string,
--pl string,
--o_id string,
--`on` string,
--cut string,
--cua string,
--pt string,
--ca string,
--ac string,
--kv_ string,
--du string,
--os string,
--os_v string,
--browser string,
--browser_v string,
--country string,
--province string,
--city string
--) partitioned by ( month String , day string )
--row format delimited fields terminated by '\001';
--
----加载数据
--load data inpath '/ods/month08/day01' into table ods_logs partition (month='${hiveconf:month}',day='${hiveconf:day}');
--
----创建获取维度Id的UDF函数,并测试
----add jar /usr/local/apache-hive-1.2.1-bin/customUDF-lib/test.jar;
--
----create temporary function date_convert as 'com.bay.analystic.hive.udf.DateDimensionUDF';
--
----create temporary function platform_convert as 'com.bay.analystic.hive.udf.PlatFormDimensionUDF';
--
----create temporary function event_convert as 'com.bay.analystic.hive.udf.EventDimensionUDF';
--
--create temporary function date_convert as 'com.bay.analystic.hive.udf.DateDimensionUDF' using jar 'hdfs://hadoop010:9000/usr/hive-jars/customUDF-lib/test.jar';
--
--create temporary function platform_convert as 'com.bay.analystic.hive.udf.PlatFormDimensionUDF' using jar 'hdfs://hadoop010:9000/usr/hive-jars/customUDF-lib/test.jar';
--
--create temporary function event_convert as 'com.bay.analystic.hive.udf.EventDimensionUDF' using jar 'hdfs://hadoop010:9000/usr/hive-jars/customUDF-lib/test.jar';
--
----创建HBase对应的Hive表
--create external table if not exists stats_event (
--`platform_dimension_id` int,
--`date_dimension_id` int,
--`event_dimension_id` int,
--`times` int,
--`created` date);
--
----创建Hive表映射每一天的数据,创建成分区表
--create external table if not exists dw_event(
--s_time string,
--en string,
--pl string,
--ca string,
--ac string
--)partitioned by(month String,day string)
--row format delimited fields terminated by '\001';
--
----导入数据
--insert into table dw_event partition (month=8,day=1)
--select s_time,en,pl,ca,ac from ods_logs where month=8 and day=1;
--
----with tmp as (
----select from_unixtime( cast ( cast(de.s_time as BIGINT) / 1000 as BIGINT), 'yyyy-MM-dd') as dt,date_convert(de.s_time) as cda,platform_convert(de.pl) as cpl,event_convert(de.ca,de.ac) as cev from dw_event as de
----where de.pl is not null and de.month='${hiveconf:month}' and de.day='${hiveconf:day}' group by de.s_time,de.pl,de.ca,de.ac )
----insert into stats_event (`platform_dimension_id`,`date_dimension_id`,`event_dimension_id`,`times`,`created`)
----select cda,cpl,cev,count(1),dt from tmp group by dt,cda,cpl,cev;
--
----扩展ll维度
--with tmp as (
--select from_unixtime( cast ( cast(de.s_time as BIGINT) / 1000 as BIGINT), 'yyyy-MM-dd') as dt,de.pl as pl,de.ca as ca,de.ac as ac from dw_event as de
--where de.pl is not null and de.month=8 and de.day=1 group by de.s_time,de.pl,de.ca,de.ac )
--from (
--select dt,pl as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,pl,ca,ac union all
--select dt,pl as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,pl,ca union all
--select dt,pl as pl,'all' as ca,'all' as ac,count(1) as ct from tmp group by dt,pl union all
--select dt,'all' as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
--select dt,'all' as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,ca union all
--select dt,'all' as pl,'all' as ca,'all' as ac,count(1) as ct from tmp group by dt
--) as tmp2
--insert into stats_event (`platform_dimension_id`,`date_dimension_id`,`event_dimension_id`,`times`,`created`)
--select platform_convert(pl),date_convert(dt),event_convert(ca,ac),sum(ct),dt group by pl,dt,ca,ac;





--sqoop export --connect jdbc:mysql://hadoop010:3306/result --username root --password 19950116 --table stats_event --export-dir hdfs://hadoop010:9000/user/hive/warehouse/stats_event --input-fields-terminated-by '\001' --update-mode allowinsert
