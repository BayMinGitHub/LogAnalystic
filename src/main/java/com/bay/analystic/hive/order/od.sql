--创建数据库
create database if not exists loganalystic;
use loganalystic;

--订单分析
--加载自定义UDF函数
create temporary function date_convert as 'com.bay.analystic.hive.udf.DateDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function platform_convert as 'com.bay.analystic.hive.udf.PlatFormDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function event_convert as 'com.bay.analystic.hive.udf.EventDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function kpi_convert as 'com.bay.analystic.hive.udf.KpiDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function currency_convert as 'com.bay.analystic.hive.udf.CurrencyTypeDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

create temporary function payment_convert as 'com.bay.analystic.hive.udf.PaymentTypeDimensionUDF' using jar 'hdfs://hadoop010:9000/user/hive-jars/customUDF-lib/LogAnalystic-1.0-SNAPSHOT.jar';

--创建Hive中的订单表(与MySQL中的对应)
create table if not exists stats_order (
`platform_dimension_id` int,
`date_dimension_id` int,
`currency_type_dimension_id` int,
`payment_type_dimension_id` int,
`orders` int,
`success_orders` int,
`refund_orders` int,
`order_amount` int,
`revenue_amount` int,
`refund_amount` int,
`total_revenue_amount` int,
`total_refund_amount` int,
`created` date )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

--成功表
create table if not exists dw_order_crt (
s_time bigint,
pl string,
oid string,
cua string,
cut string,
pt string
) partitioned by (month int,day int) stored as orc;

--导入数据
insert overwrite table dw_order_crt partition (month=08,day=01)
select s_time,pl,o_id,cua,cut,pt from ods_logs where month=08 and day=01 and en='e_crt';

--退款
create table if not exists dw_order_cr (
s_time bigint,
pl string,
oid string,
cua string,
cut string,
pt string
) partitioned by (month int,day int) stored as orc;

--导入数据
insert overwrite table dw_order_cr partition (month=08,day=01)
select s_time,pl,o_id,cua,cut,pt from ods_logs where month=08 and day=01 and en='e_cr';

--抽取数据存入最终表
with tmp as (
select platform_convert(pl) as pl,date_convert(from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd")) as dt,currency_convert(cut) as cut,payment_convert(pt) as pt,
count(oid) as s_orders,0 as r_orders,sum(cast(cua as double)) as o_amount,sum(cast(cua as double)) as s_amount,0 as r_amount,0 as t_r_amount,0 as t_re_amount,from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd") as created
from dw_order_crt where month=08 and day=01 group by pl,s_time,cut,pt union all
select platform_convert(pl) as pl,date_convert(from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd")) as dt,currency_convert(cut) as cut,payment_convert(pt) as pt,
0 as s_orders,count(oid) as r_orders,0 as o_amount,0 as s_amount,sum(cast(cua as double)) as r_amount,0 as t_r_amount,0 as t_re_amount,from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd")as created
from dw_order_cr where month=08 and day=01 group by pl,s_time,cut,pt )
insert overwrite table stats_order
select pl,date_convert(created),cut,pt,(sum(s_orders) + sum(r_orders)),sum(s_orders),sum(r_orders),sum(o_amount),sum(s_amount),sum(r_amount),sum(t_r_amount),sum(t_re_amount),created from tmp group by pl,cut,pt,created;