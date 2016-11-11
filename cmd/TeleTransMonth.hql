set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;
set mapreduce.jobtracker.split.metainfo.maxsize = -1

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
from tbl_common_his_trans where pdate>='20150401' and pdate<='20150531';

insert overwrite directory 'TeleTrans/Sum04'
select * from(  
select tfr_in_acct_no, tfr_out_acct_no, sum(trans_at) as amount, count(trans_at) as count
from TeleTransMonth
group by tfr_in_acct_no, tfr_out_acct_no
)tmp2;


 
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
count(substr(acpt_ins_id_cd,5,4)) as locsumDay, count(trans_md='2') as foreignDay
from trans0405
group by tfr_in_acct_no, pdate;
 
 


 
 
CREATE TABLE IF NOT EXISTS AvgByDay(
tfr_in_acct_no string,
dam double, 
dac double,
dacv double
);

 
 
INSERT OVERWRITE TABLE AvgByDay 
select tfr_in_acct_no, avg(msumDay) as dam, avg(csumDay) as dac, avg(cvDay) as dacv
from StaticByDay
group by tfr_in_acct_no;
 
 
 
CREATE TABLE IF NOT EXISTS AbnDay(
tfr_in_acct_no string,
pdate string,

msumDay double, 
csumDay int,
cvDay double,

dam double,
dac double,
dacv double,

mRatio double,
cRatio double,
cvRatio double
); 
 
INSERT OVERWRITE TABLE AbnDay
select t1.tfr_in_acct_no, t1.pdate, t1.msumDay, t1.csumDay, t1.cvDay, t2.dam, t2.dac, t2.dacv,
  t1.msumDay/t2.dam as mRatio, t1.csumDay/t2.dac as cRatio, t1.cvDay/t2.dacv as cvRatio
from StaticByDay t1
join AvgByDay t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no
distribute by t1.tfr_in_acct_no
sort by t1.tfr_in_acct_no, t1.pdate;
 

   
CREATE TABLE IF NOT EXISTS AbnTest(
tfr_in_acct_no string,
pdate string,

msumDay double, 
csumDay int,
cvDay double,

dam double,
dac double,
dacv double,

mRatio double,
cRatio double,
cvRatio double
); 

INSERT OVERWRITE TABLE AbnTest
select t1.tfr_in_acct_no, t1.pdate, t1.msumDay, t1.csumDay, t1.cvDay, t2.dam, t2.dac, t2.dacv,
  t1.msumDay/t2.dam as mRatio, t1.csumDay/t2.dac as cRatio, t1.cvDay/t2.dacv as cvRatio
from StaticByDay t1
join AvgByDay t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no
distribute by t1.tfr_in_acct_no
sort by t1.tfr_in_acct_no, mRatio DESC; 
 
 
 
select count(case when trans_md ='1' then 1 else null end), count(case when trans_md ='2' then 1 else null end) from trans0405; 
 