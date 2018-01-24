hive -e "
create table if not exists badaccounts(account string)
row format delimited fields terminated by '\001'
lines terminated by '\n';
load data local inpath 'BadCards.txt' into table badaccounts;


create table if not exists normalaccounts(account string);
insert overwrite table normalaccounts
select tfr_in_acct_no from tbl_common_his_trans
where trans_id='S33' and pdate>'20150301' and pdate<'20150501' and tfr_in_acct_no not in (
arlab.hmacmd5(reverse('166226681400322972')),
arlab.hmacmd5(reverse('166226681400167807')),
arlab.hmacmd5(reverse('166226731400040122')),
arlab.hmacmd5(reverse('166226631400042146')),
arlab.hmacmd5(reverse('166226681400321289')),
arlab.hmacmd5(reverse('166226681400291300')),
arlab.hmacmd5(reverse('166226681400211019')),
arlab.hmacmd5(reverse('166226681400321289')),
arlab.hmacmd5(reverse('166226631201740039')),
arlab.hmacmd5(reverse('166226631400172018')))
order by rand() limit 50;
"

hive -e "insert overwrite local directory 'IniMergedCards'
select * from(  
select t1.tfr_in_acct_no, t1.tfr_out_acct_no
from tbl_common_his_trans t1
left semi join badaccounts t2
on t1.tfr_in_acct_no=arlab.hmacmd5(reverse(t2.account))
where t1.trans_id='S33' and t1.pdate>'20150301' and pdate<'20150501'
union all
select t1.tfr_in_acct_no, t1.tfr_out_acct_no
from tbl_common_his_trans t1
left semi join badaccounts t2
on t1.tfr_out_acct_no=arlab.hmacmd5(reverse(t2.account))
where t1.trans_id='S33' and t1.pdate>'20150301' and pdate<'20150501'
union all
select t1.tfr_in_acct_no, t1.tfr_out_acct_no
from tbl_common_his_trans t1
left semi join normalaccounts t2
on t1.tfr_in_acct_no=t2.account
where t1.trans_id='S33' and t1.pdate>'20150301' and pdate<'20150501'
union all
select t1.tfr_in_acct_no, t1.tfr_out_acct_no
from tbl_common_his_trans t1
left semi join normalaccounts t2
on t1.tfr_out_acct_no=t2.account
where t1.trans_id='S33' and t1.pdate>'20150301' and pdate<'20150501'
) tmp;
"   

hive -e "
drop table if exists badaccounts;
drop table if exists normalaccounts;
"
