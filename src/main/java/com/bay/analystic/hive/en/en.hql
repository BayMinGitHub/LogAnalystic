--创建Hive分区表映射每一天的数据
create external table if not exists ods_logs (
s_time string,
en string,
ver string,
u_ud string,
u_mid string,
u_sid string,
c_time string,
language  string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
o_id string,
`on` string,
cut string,
cua string,
pt string,
ca string,
ac string,
kv_ string,
du string,
os string,
os_v string,
browser string,
browser_v string,
country string,
province string,
city string
) partitioned by ( month String , day string )
row format delimited fields terminated by '\001';

--加载数据
load data inpath '/ods/month08/day01' into table ods_logs partition (month='${hiveconf:month}',day='${hiveconf:day}');

--创建HBase对应的Hive表
create external table if not exists stats_event (
`platform_dimension_id` int,
`date_dimension_id` int,
`event_dimension_id` int,
`times` int,
`created` date);

add jar /usr/local/apache-hive-1.2.1-bin/customUDF-lib/test.jar;

create temporary function date_convert as 'com.bay.analystic.hive.udf.DateDimensionUDF';

create temporary function platform_convert as 'com.bay.analystic.hive.udf.PlatFormDimensionUDF';

create temporary function event_convert as 'com.bay.analystic.hive.udf.EventDimensionUDF';

--with tmp as (
--select s_time as dt,pl,ca,ac,count(1) as cts from ods_logs
--where pl is not null and month='${hiveconf:month}' and day='${hiveconf:day}' group by s_time,pl,ca,ac )
--insert into stats_event (`platform_dimension_id`,`date_dimension_id`,`event_dimension_id`,`times`,`created`)
--select date_convert(dt),platform_convert(pl),event_convert(ca,ac),cts,from_unixtime( cast ( cast(dt as BIGINT) / 1000 as BIGINT), 'yyyy-MM-dd') from tmp;


with tmp as (
select from_unixtime( cast ( cast(s_time as BIGINT) / 1000 as BIGINT), 'yyyy-MM-dd') as dt,date_convert(s_time) as cda,platform_convert(pl) as cpl,event_convert(ca,ac) as cev from ods_logs
where pl is not null and month=8 and day=1 group by s_time,pl,ca,ac )
insert into stats_event (`platform_dimension_id`,`date_dimension_id`,`event_dimension_id`,`times`,`created`)
select cda,cpl,cev,count(1),dt from tmp group by dt,cda,cpl,cev;


--sqoop export --connect jdbc:mysql://hadoop010:3306/result -username root --password 19950116 --table stats_event --export-dir /user/hive/warehouse/stats_event --input-fields-terminated-by '\001'
