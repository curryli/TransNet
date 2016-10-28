insert overwrite directory 'xrli/sortamountql'
select * from(  
select tfr_out_acct_no, sum(trans_at) as amount
from tbl_common_his_trans
where trans_id='S33' and pdate>'20150301' and pdate<'20150401'
group by tfr_out_acct_no
cluster by amount
)tmp2;