INSERT OVERWRITE TABLE TeleTransSort 
select tfr_out_acct_no, tfr_in_acct_no, trans_at, pdate, loc_trans_tm 
from TeleTrans where pdate>='20150801' and pdate<='20161031' 
distribute by tfr_out_acct_no 
sort by pdate, loc_trans_tm	