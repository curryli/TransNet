set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;


insert overwrite directory 'TeleTrans/Trans1516'
select * from(  
select tfr_out_acct_no, tfr_in_acct_no, trans_at, pdate, loc_trans_tm, source_region_cd, dest_region_cd, cross_dist_in
from tbl_common_his_trans
where trans_id='S33' and pdate>='20150706' and pdate<'20160626'
)tmp;
