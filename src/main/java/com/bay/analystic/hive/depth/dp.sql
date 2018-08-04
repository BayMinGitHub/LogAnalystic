--创建数据库
create database if not exists loganalystic;
use loganalystic;

--用户深度分析
--加载自定义UDF函数
create temporary function date_convert as 'com.bay.analystic.hive.udf.DateDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function platform_convert as 'com.bay.analystic.hive.udf.PlatFormDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function event_convert as 'com.bay.analystic.hive.udf.EventDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function kpi_convert as 'com.bay.analystic.hive.udf.KpiDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

--创建Hive中的深度表(与Mysql中的对应)
create table if not exists stats_view_depth (
`platform_dimension_id` int,
`date_dimension_id` int,
`kpi_dimension_id` int,
`pv1` int,
`pv2` int,
`pv3` int,
`pv4` int,
`pv5_10` int,
`pv10_30` int,
`pv30_60` int,
`pv60plus` int,
`created` date);

--创建临时表,并抽取数据
create table if not exists dw_depth(
s_time bigint,
pl string,
p_url string,
u_ud string,
u_sd string
) partitioned by (month int,day int) stored as orc;

--导入数据
insert overwrite table dw_depth partition (month='${hiveconf:month}',day='${hiveconf:day}')
select s_time,pl,p_url,u_ud,u_sid from ods_logs where month='${hiveconf:month}' and day='${hiveconf:day}' and en='e_pv';

--创建临时表
create table if not exists dwa_depth (
pl string,
dt string,
col string,
ct int );

--向临时表存储抽取的数据
with tmp as (
select from_unixtime( cast ( dd.s_time / 1000 as bigint ),"yyyy-MM-dd" ) as dt,dd.pl as pl,
( case
when count(dd.p_url) = 1 then 'pv1'
when count(dd.p_url) = 2 then 'pv2'
when count(dd.p_url) = 3 then 'pv3'
when count(dd.p_url) = 4 then 'pv4'
when count(dd.p_url) <= 10 then 'pv5_10'
when count(dd.p_url) <= 30 then 'pv10_30'
when count(dd.p_url) <= 60 then 'pv30_60'
else 'pv60plus' end ) as pv,dd.u_ud as u_ud
from dw_depth as dd where pl is not null and dd.p_url is not null and dd.month='${hiveconf:month}' and dd.day='${hiveconf:day}'
group by pl,from_unixtime( cast ( dd.s_time / 1000 as bigint ),"yyyy-MM-dd" ),u_ud )
insert overwrite table dwa_depth
select pl,dt,pv,count(u_ud) from tmp group by pl,dt,pv;

--扩展all维度
with tmp as (
select pl as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv1' union all
select pl as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv2' union all
select pl as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv3' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv4' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv5_10' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv10_30' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plus from dwa_depth where col='pv30_60' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plus from dwa_depth where col='pv60plus' union all
select 'all'as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv1' union all
select 'all'as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv2' union all
select 'all'as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv3' union all
select 'all'as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv4' union all
select 'all'as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv5_10' union all
select 'all'as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plus from dwa_depth where col='pv10_30' union all
select 'all'as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plus from dwa_depth where col='pv30_60' union all
select 'all'as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plus from dwa_depth where col='pv60plus' )
insert overwrite table stats_view_depth
select platform_convert(pl),date_convert(dt),kpi_convert('depth_view_user'),sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plus),dt
from tmp group by dt,pl;