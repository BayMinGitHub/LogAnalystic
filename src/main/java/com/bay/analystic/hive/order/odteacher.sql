create table if not exists dw_od (
s_time bigint,
pl string,
oid string,
cut string,
cua string,
pt string,
en string ) partitioned by (month int,day int) stored as orc;

from ods_logs
insert overwrite table dw_od partition(month=08,day=01)
select s_time,pl,o_id,cut,cua,pt
where month=08 and day=01 and o_id is not null and oid != 'null';

select from_unixtime(cast(do.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
do.pl as pl,
do.cut as cut,
do.pt as pt,
count(distinct do.oid) as ct
from dw_od where do.month=08 and day=01
group by from_unixtime(cast(do.s_time/1000 as bigint),'yyyy-MM-dd'),pl,cut,pt;