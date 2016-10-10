insert overwrite directory 'xrli/AllTrans'
select * from(  
select tfr_out_acct_no, tfr_in_acct_no, sum(trans_at) as amount, count(trans_at) as count
from tbl_common_his_trans
where trans_id='S33' and pdate>'20150301' and pdate<'20150401'
group by tfr_in_acct_no, tfr_out_acct_no
)tmp2;