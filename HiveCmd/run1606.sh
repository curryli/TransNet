hive -e"CREATE TABLE IF NOT EXISTS Delta1606(
tfr_in_acct_no string,
pdate string,
DeltaT double, 
DeltaM double, 
Deltamt double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
 
load data inpath 'TeleTrans/Delta1606' into table Delta1606;

CREATE TABLE IF NOT EXISTS StaticDelta1606(
tfr_in_acct_no string,
pdate string,
AvgDT double
);

INSERT OVERWRITE TABLE StaticDelta1606
select tfr_in_acct_no, pdate, avg(DeltaT) as AvgDT
from Delta1606
where DeltaT is not NULL
group by tfr_in_acct_no, pdate;

CREATE TABLE IF NOT EXISTS StaticByDay1606(
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

INSERT OVERWRITE TABLE StaticByDay1606
select tfr_in_acct_no, pdate, sum(trans_at) as msumDay, count(trans_at) as csumDay, 
avg(trans_at) as avgDay, stddev(trans_at) as stdDay, stddev(trans_at)/avg(trans_at) as cvDay,
count(distinct(substr(acpt_ins_id_cd,5,4))) as locsumDay, count(case when trans_md='2' then 1 else null end) as foreignDay
from tele_use1606
group by tfr_in_acct_no, pdate;

CREATE TABLE IF NOT EXISTS DayInfo1606(
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

INSERT OVERWRITE TABLE DayInfo1606
select t1.tfr_in_acct_no, t1.pdate, t1.msumDay, t1.csumDay, t1.avgDay, t1.stdDay,
t1.cvDay, t1.locsumDay, t1.foreignDay, t2.AvgDT
from StaticByDay1606 t1
left outer join StaticDelta1606 t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no and t1.pdate = t2.pdate; 

CREATE TABLE IF NOT EXISTS AvgByDay1606(
tfr_in_acct_no string,
dam double, 
dac double,
dacv double,
daADT double,
dloc int,
dforeign int
);

INSERT OVERWRITE TABLE AvgByDay1606 
select tfr_in_acct_no, avg(msumDay) as dam, avg(csumDay) as dac, avg(cvDay) as dacv, avg(AvgDT) as daADT, 
avg(locsumDay) as dloc, avg(foreignDay) as dforeign
from DayInfo1606
group by tfr_in_acct_no;
 
CREATE TABLE IF NOT EXISTS AbnDay1606(
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
 
INSERT OVERWRITE TABLE AbnDay1606
select t1.tfr_in_acct_no, t1.pdate, t1.locsumDay, t1.foreignDay, t1.msumDay, t1.csumDay, t1.cvDay, t1.AvgDT, 
t2.dam, t2.dac, t2.dacv, t2.daADT, t1.msumDay/t2.dam as mRatio, 
t1.csumDay/t2.dac as cRatio, t1.cvDay/t2.dacv as cvRatio, t1.AvgDT/t2.daADT as ADTRatio
from DayInfo1606 t1
join AvgByDay1606 t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no
distribute by t1.tfr_in_acct_no
sort by t1.tfr_in_acct_no, t1.pdate;

CREATE TABLE IF NOT EXISTS StaticDeltamt1606(
tfr_in_acct_no string,
pdate string,
Deltamt double
);
 
INSERT OVERWRITE TABLE StaticDeltamt1606
select tfr_in_acct_no, pdate, Deltamt from Delta1606
where Deltamt is not NULL
order by Deltamt DESC;

CREATE TABLE IF NOT EXISTS StaticAll1606(
tfr_in_acct_no string,
msum double, 
csum int,
mavg double,
mstd double,
mcv double,
locsum int,
foreignsum int
);

INSERT OVERWRITE TABLE StaticAll1606
select tfr_in_acct_no, sum(trans_at) as msum, count(trans_at) as csum, 
avg(trans_at) as mavg, stddev(trans_at) as mstd, stddev(trans_at)/avg(trans_at) as mcv,
count(distinct(substr(acpt_ins_id_cd,5,4))) as locsum, count(case when trans_md='2' then 1 else null end) as foreignsum
from tele_use1606
group by tfr_in_acct_no; 

CREATE TABLE IF NOT EXISTS AvgDelta1606(
tfr_in_acct_no string,
AvgDT double
);

INSERT OVERWRITE TABLE AvgDelta1606
select tfr_in_acct_no, avg(DeltaT) as AvgDT
from Delta1606
where DeltaT is not NULL
group by tfr_in_acct_no;

CREATE TABLE IF NOT EXISTS AllInfo1606(
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

INSERT OVERWRITE TABLE AllInfo1606
select t2.*, t1.AvgDT
from AvgDelta1606 t1
right outer join StaticAll1606 t2
on t1.tfr_in_acct_no = t2.tfr_in_acct_no;

CREATE TABLE IF NOT EXISTS StaticOut1606(
tfr_out_acct_no string,
msum double, 
csum int,
locsum int,
foreignsum int
);

INSERT OVERWRITE TABLE StaticOut1606
select tfr_out_acct_no, sum(trans_at) as msum, count(trans_at) as csum, 
count(distinct(substr(acpt_ins_id_cd,5,4))) as locsum, count(case when trans_md='2' then 1 else null end) as foreignsum
from tele_use1606
group by tfr_out_acct_no; 

CREATE TABLE IF NOT EXISTS transMD51606(
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

INSERT OVERWRITE TABLE transMD51606
select *, hash_md5_int(tfr_in_acct_no) as in_MD5, hash_md5_int(tfr_out_acct_no) as out_MD5
from tele_use1606;

CREATE TABLE IF NOT EXISTS StaticMD51606(
tfr_in_acct_no string,
in_MD5 string,
tfr_out_acct_no string,
out_MD5 string,
msum double, 
csum int,
locsum int,
foreignsum int
);

INSERT OVERWRITE TABLE StaticMD51606
select tfr_in_acct_no, hash_md5_int(tfr_in_acct_no) as in_MD5, tfr_out_acct_no, hash_md5_int(tfr_out_acct_no) as out_MD5,
sum(trans_at) as msum, count(trans_at) as csum, count(distinct(substr(acpt_ins_id_cd,5,4))) as locsum, 
count(case when trans_md='2' then 1 else null end) as foreignsum
from tele_use1606
group by tfr_in_acct_no, tfr_out_acct_no;" 