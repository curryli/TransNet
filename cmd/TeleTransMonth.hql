set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;
set mapreduce.jobtracker.split.metainfo.maxsize = -1

CREATE TABLE IF NOT EXISTS TeleTransMonth(
tfr_in_acct_no string,
tfr_out_acct_no string,
trans_at double, 
pdate string,
loc_trans_tm string,
acpt_ins_id_cd string,
trans_md string,
cross_dist_in string
);

INSERT OVERWRITE TABLE TeleTransMonth 
select tfr_in_acct_no, tfr_out_acct_no, trans_at, pdate, loc_trans_tm, acpt_ins_id_cd, trans_md, cross_dist_in
from tbl_common_his_trans where pdate>='20150406' and pdate<='20150503' 
distribute by tfr_out_acct_no 
sort by pdate, loc_trans_tm;