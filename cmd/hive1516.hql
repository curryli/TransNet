set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;


insert overwrite directory 'TeleTrans/Trans1516'
select * from(  
select tfr_out_acct_no, tfr_in_acct_no, trans_at, pdate, loc_trans_tm, source_region_cd, dest_region_cd, cross_dist_in
from tbl_common_his_trans
where trans_id='S33' and pdate>='20150706' and pdate<='20160626'
)tmp;


转出帐户，转入账户，金额，本地交易日期，本地交易时间，源地区代码，目的地区代码，是否跨境





CREATE TABLE IF NOT EXISTS TeleTrans(  
tfr_out_acct_no string, 
tfr_in_acct_no string, 
trans_at double, 
pdate string, 
loc_trans_tm string, 
source_region_cd string, 
dest_region_cd string, 
cross_dist_in string
);  


alter table TeleTrans change trans_at trans_at DOUBLE;


load data inpath 'TeleTrans/Trans1516' into table TeleTrans;
load data inpath 'TeleTrans/Trans1516' into table TeleTrans;  



CREATE TABLE IF NOT EXISTS TeleTransWeek(  
tfr_out_acct_no string, 
pdate string, 
amount double,
count int
);
 


INSERT OVERWRITE TABLE TeleTransWeek
select tfr_out_acct_no, pdate, sum(trans_at) as amount, count(trans_at) as count
from TeleTrans
group by tfr_out_acct_no, pdate;



 
 

SELECT a.tfr_in_acct_no FROM TeleTransWeek a JOIN b ON (a.key = b.key1) 


alter table TeleTransWeek change amount amount DOUBLE;
alter table TeleTransWeek change count count INT;