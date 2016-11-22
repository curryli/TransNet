set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;
set mapreduce.jobtracker.split.metainfo.maxsize = -1


-- 提取时间段所有数据方便后续操作
CREATE TABLE IF NOT EXISTS trans0405(
tfr_in_acct_no string,
tfr_out_acct_no string,
trans_at double, 
pdate string,
loc_trans_tm string,
acpt_ins_id_cd string,
trans_md string,
cross_dist_in string
);
 
INSERT OVERWRITE TABLE trans0405
select tfr_in_acct_no, tfr_out_acct_no, trans_at, pdate, loc_trans_tm, acpt_ins_id_cd, trans_md, cross_dist_in
from tbl_common_his_trans 
where trans_id='S33' and pdate>='20150401' and pdate<='20150531';


-- 将MR SecondarySort排序好的结果计算上下两笔交易差的结果建表 
CREATE TABLE IF NOT EXISTS Delta0405(
tfr_in_acct_no string,
pdate string,
DeltaT double, 
DeltaM double, 
Deltamt double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
 
load data inpath 'TeleTrans/Delta0405' into table Delta0405; 


1、	将时间T内，每天进行聚合统计。
-- 以天为单位，计算针对卡号的每天上下笔交易的平均时间差 
CREATE TABLE IF NOT EXISTS StaticDelta(
tfr_in_acct_no string,
pdate string,
AvgDT double
);

INSERT OVERWRITE TABLE StaticDelta
select tfr_in_acct_no, pdate, avg(DeltaT) as AvgDT
from Delta0405
where DeltaT is not NULL
group by tfr_in_acct_no, pdate;



-- 以天为单位进行聚合统计
CREATE TABLE IF NOT EXISTS StaticByDay(
tfr_in_acct_no string,
pdate string,
msumDay double, 
csumDay int,
avgDay double,
stdDay double,
cvDay double,
locsumDay int,
foreignDay int
);

INSERT OVERWRITE TABLE StaticByDay
select tfr_in_acct_no, pdate, sum(trans_at) as msumDay, count(trans_at) as csumDay, 
avg(trans_at) as avgDay, stddev(trans_at) as stdDay, stddev(trans_at)/avg(trans_at) as cvDay,
count(distinct(substr(acpt_ins_id_cd,5,4))) as locsumDay, count(case when trans_md='2' then 1 else null end) as foreignDay
from trans0405
group by tfr_in_acct_no, pdate;
 


 
-- 以天为单位进行聚合统计（加入一天上下笔交易的平均时间差）
CREATE TABLE IF NOT EXISTS DayInfo(
tfr_in_acct_no string,
pdate string,
msumDay double, 
csumDay int,
avgDay double,
stdDay double,
cvDay double,
locsumDay int,
foreignDay int,
AvgDT double
);

INSERT OVERWRITE TABLE DayInfo
select t1.tfr_in_acct_no, t1.pdate, t1.msumDay, t1.csumDay, t1.avgDay, t1.stdDay,
t1.cvDay, t1.locsumDay, t1.foreignDay, t2.AvgDT
from StaticByDay t1
join StaticDelta t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no and t1.pdate = t2.pdate; 

 
 
-- 在整个时间段内，统计针对卡号的平均每天交易金额、平均每天交易次数，平均每天离散系数，平均每天交易的平均时间差
CREATE TABLE IF NOT EXISTS AvgByDay(
tfr_in_acct_no string,
dam double, 
dac double,
dacv double,
daADT double
);

INSERT OVERWRITE TABLE AvgByDay 
select tfr_in_acct_no, avg(msumDay) as dam, avg(csumDay) as dac, avg(cvDay) as dacv, avg(AvgDT) as daADT
from DayInfo
group by tfr_in_acct_no;
 
 
-- 在整个时间段内,仍然以卡号和天为单位聚合，增加相应的 天聚合变量/平均数 比例，可以帮助发现异常天数
CREATE TABLE IF NOT EXISTS AbnDay(
tfr_in_acct_no string,
pdate string,

locsumDay int,
foreignDay int,

msumDay double, 
csumDay int,
cvDay double,
AvgDT double,

dam double,
dac double,
dacv double,
daADT double,

mRatio double,
cRatio double,
cvRatio double,
ADTRatio double
); 
 
INSERT OVERWRITE TABLE AbnDay
select t1.tfr_in_acct_no, t1.pdate, t1.locsumDay, t1.foreignDay, t1.msumDay, t1.csumDay, t1.cvDay, t1.AvgDT, 
t2.dam, t2.dac, t2.dacv, t2.daADT, t1.msumDay/t2.dam as mRatio, 
t1.csumDay/t2.dac as cRatio, t1.cvDay/t2.dacv as cvRatio, t1.AvgDT/t2.daADT as ADTRatio
from DayInfo t1
join AvgByDay t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no
distribute by t1.tfr_in_acct_no
sort by t1.tfr_in_acct_no, t1.pdate;
 

-- 在整个时间段内,仍然以卡号和天为单位聚合，增加相应的 天聚合变量/平均数 比例，可以帮助发现异常天数  

select * from AbnDay
sort by t1.tfr_in_acct_no, mRatio DESC limit 100; 
 
 

 
--  对上下两笔   金额差/上下两笔时间差  排序可查看select * from StaticDeltamt limit 20000;
CREATE TABLE IF NOT EXISTS StaticDeltamt(
tfr_in_acct_no string,
pdate string,
Deltamt double
);
 
INSERT OVERWRITE TABLE StaticDeltamt
select tfr_in_acct_no, pdate, Deltamt from Delta0405
where Deltamt is not NULL
order by Deltamt DESC; 
  
 
select count(case when trans_md ='1' then 1 else null end), count(case when trans_md ='2' then 1 else null end) from trans0405; 
 
 
2、 在整个时间T内，对每个账户进行聚合统计
CREATE TABLE IF NOT EXISTS StaticAll(
tfr_in_acct_no string,
msum double, 
csum int,
mavg double,
mstd double,
mcv double,
locsum int,
foreignsum int
);

INSERT OVERWRITE TABLE StaticAll
select tfr_in_acct_no, sum(trans_at) as msum, count(trans_at) as csum, 
avg(trans_at) as mavg, stddev(trans_at) as mstd, stddev(trans_at)/avg(trans_at) as mcv,
count(distinct(substr(acpt_ins_id_cd,5,4))) as locsum, count(case when trans_md='2' then 1 else null end) as foreignsum
from trans0405
group by tfr_in_acct_no; 


-- 计算整个时间段内上下笔交易的平均时间差 
CREATE TABLE IF NOT EXISTS AvgDelta(
tfr_in_acct_no string,
AvgDT double
);

INSERT OVERWRITE TABLE AvgDelta
select tfr_in_acct_no, avg(DeltaT) as AvgDT
from Delta0405
where DeltaT is not NULL
group by tfr_in_acct_no;

-- 加一列AvgDT
CREATE TABLE IF NOT EXISTS AllInfo(
tfr_in_acct_no string,
msum double, 
csum int,
mavg double,
mstd double,
mcv double,
locsum int,
foreignsum int,
AvgDT double
);

INSERT OVERWRITE TABLE AllInfo
select t2.*, t1.AvgDT
from AvgDelta t1
join StaticAll t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no;


-- 统计转出总金额、总次数
CREATE TABLE IF NOT EXISTS StaticOut(
tfr_out_acct_no string,
msum double, 
csum int,
locsum int,
foreignsum int
);

INSERT OVERWRITE TABLE StaticOut
select tfr_out_acct_no, sum(trans_at) as msum, count(trans_at) as csum, 
count(distinct(substr(acpt_ins_id_cd,5,4))) as locsum, count(case when trans_md='2' then 1 else null end) as foreignsum
from trans0405
group by tfr_out_acct_no; 

--统计净差  很慢，尽量不用
CREATE TABLE IF NOT EXISTS StaticInOut(
tfr_out_acct_no string,
MsumAll double, 
NetIncome double,
CsumAll int,
foreignAll int
);

INSERT OVERWRITE TABLE StaticInOut
select t1.tfr_in_acct_no, (t1.msum + t2.msum) as MsumAll, (t1.msum - t2.msum) as NetIncome, 
(t1.csum + t2.csum) as CsumAll, (t1.foreignsum + t2.foreignsum) as foreignAll
from StaticAll t1
join StaticOut t2;


--MD5赋值 每笔交易
CREATE TABLE IF NOT EXISTS transMD5(
tfr_in_acct_no string,
tfr_out_acct_no string,
trans_at double, 
pdate string,
loc_trans_tm string,
acpt_ins_id_cd string,
trans_md string,
cross_dist_in string,
in_MD5 string,
out_MD5 string
);

INSERT OVERWRITE TABLE transMD5
select *, hash_md5_int(tfr_in_acct_no) as in_MD5, hash_md5_int(tfr_out_acct_no) as out_MD5
from trans0405;

--MD5赋值 统计交易
CREATE TABLE IF NOT EXISTS StaticMD5(
tfr_in_acct_no string,
in_MD5 string,
tfr_out_acct_no string,
out_MD5 string,
msum double, 
csum int,
locsum int,
foreignsum int
);

INSERT OVERWRITE TABLE StaticMD5
select tfr_in_acct_no, hash_md5_int(tfr_in_acct_no) as in_MD5, tfr_out_acct_no, hash_md5_int(tfr_out_acct_no) as out_MD5,
sum(trans_at) as msum, count(trans_at) as csum, count(distinct(substr(acpt_ins_id_cd,5,4))) as locsum, 
count(case when trans_md='2' then 1 else null end) as foreignsum
from trans0405
group by tfr_in_acct_no, tfr_out_acct_no; 
