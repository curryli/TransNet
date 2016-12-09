hive -e "set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=10240000000;
set mapred.min.split.size.per.node=10240000000;
set mapred.min.split.size.per.rack=10240000000;
set mapreduce.jobtracker.split.metainfo.maxsize = -1;
set mapreduce.job.queuename=root.queue2;


create table if not exists fraudCards(card string);
load data local inpath 'fraudCards.txt' into table fraudCards;


create table if not exists fraudCards_md5(card string);

INSERT OVERWRITE TABLE fraudCards_md5
select arlab.hmacmd5(reverse(concat(length(card),card))) from fraudCards;

drop table fraudCards;

select * from SuspectCards t1
left semi join fraudCards_md5 t2
on t1.card=t2.card;


///////
select * from tele_trans1516 t1
left semi join fraudCards_md5 t2
on t1.tfr_in_acct_no=t2.card
union all
select * from tele_trans1516 t1
left semi join fraudCards_md5 t2
on t1.tfr_in_acct_no=t2.card
////////////



select * from tbl_common_his_trans t1 
left semi join fraudCards_md5 t2
on t1.tfr_in_acct_no=t2.card
where t1.trans_id='S33' and t1.pdate>='20160101' and t1.pdate<='20160631'; 



CREATE TABLE IF NOT EXISTS tele_fraud_1011(
pri_key                 string,   
log_cd                  string,   
settle_tp               string,   
settle_cycle            string,   
block_id                string,   
orig_key                string,   
related_key             string,   
trans_fwd_st            string,   
trans_rcv_st            string,   
sms_dms_conv_in         string,   
fee_in                  string,   
cross_dist_in           string,   
orig_acpt_sdms_in       string,   
tfr_in_in               string,   
trans_md                string,   
source_region_cd        string,   
dest_region_cd          string,   
cups_card_in            string,   
cups_sig_card_in        string,   
card_class              string,   
card_attr               string,   
sti_in                  string,   
trans_proc_in           string,   
acq_ins_id_cd           string,   
acq_ins_tp              string,   
fwd_ins_id_cd           string,   
fwd_ins_tp              string,   
rcv_ins_id_cd           string,   
rcv_ins_tp              string,   
iss_ins_id_cd           string,   
iss_ins_tp              string,   
related_ins_id_cd       string,   
related_ins_tp          string,   
acpt_ins_id_cd          string,   
acpt_ins_tp             string,   
pri_acct_no             string,   
pri_acct_no_conv        string,   
sys_tra_no              string,   
sys_tra_no_conv         string,   
sw_sys_tra_no           string,   
auth_dt                 string,   
auth_id_resp_cd         string,   
resp_cd1                string,   
resp_cd2                string,   
resp_cd3                string,   
resp_cd4                string,   
cu_trans_st             string,   
sti_takeout_in          string,   
trans_id                string,   
trans_tp                string,   
trans_chnl              string,   
card_media              string,   
card_media_proc_md      string,   
card_brand              string,   
expire_seg              string,   
trans_id_conv           string,   
settle_dt               string,   
settle_mon              string,   
settle_d                string,   
orig_settle_dt          string,   
settle_fwd_ins_id_cd    string,   
settle_rcv_ins_id_cd    string,   
trans_at                string,   
orig_trans_at           string,   
trans_conv_rt           string,   
trans_curr_cd           string,   
cdhd_fee_at             string,   
cdhd_fee_conv_rt        string,   
cdhd_fee_acct_curr_cd   string,   
repl_at                 string,   
exp_snd_chnl            string,   
confirm_exp_chnl        string,   
extend_inf              string,   
conn_md                 string,   
msg_tp                  string,   
msg_tp_conv             string,   
card_bin                string,   
related_card_bin        string,   
trans_proc_cd           string,   
trans_proc_cd_conv      string,   
tfr_dt_tm               string,   
loc_trans_tm            string,   
loc_trans_dt            string,   
conv_dt                 string,   
mchnt_tp                string,   
pos_entry_md_cd         string,   
card_seq                string,   
pos_cond_cd             string,   
pos_cond_cd_conv        string,   
retri_ref_no            string,   
term_id                 string,   
term_tp                 string,   
mchnt_cd                string,   
card_accptr_nm_addr     string,   
ic_data                 string,   
rsn_cd                  string,   
addn_pos_inf            string,   
orig_msg_tp             string,   
orig_msg_tp_conv        string,   
orig_sys_tra_no         string,   
orig_sys_tra_no_conv    string,   
orig_tfr_dt_tm          string,   
related_trans_id        string,   
related_trans_chnl      string,   
orig_trans_id           string,   
orig_trans_id_conv      string,   
orig_trans_chnl         string,   
orig_card_media         string,   
orig_card_media_proc_md string,   
tfr_in_acct_no          string,   
tfr_out_acct_no         string,   
cups_resv               string,   
ic_flds                 string,   
cups_def_fld            string,   
spec_settle_in          string,   
settle_trans_id         string,   
spec_mcc_in             string,   
iss_ds_settle_in        string,   
acq_ds_settle_in        string,   
settle_bmp              string,   
upd_in                  string,   
exp_rsn_cd              string,   
to_ts                   string,   
resnd_num               string,   
pri_cycle_no            string,   
alt_cycle_no            string,   
corr_pri_cycle_no       string,   
corr_alt_cycle_no       string,   
disc_in                 string,   
vfy_rslt                string,   
vfy_fee_cd              string,   
orig_disc_in            string,   
orig_disc_curr_cd       string,   
fwd_settle_at           string,   
rcv_settle_at           string,   
fwd_settle_conv_rt      string,   
rcv_settle_conv_rt      string,   
fwd_settle_curr_cd      string,   
rcv_settle_curr_cd      string,   
disc_cd                 string,   
allot_cd                string,   
total_disc_at           string,   
fwd_orig_settle_at      string,   
rcv_orig_settle_at      string,   
vfy_fee_at              string,   
sp_mchnt_cd             string,   
acct_ins_id_cd          string,   
iss_ins_id_cd1          string,   
iss_ins_id_cd2          string,   
iss_ins_id_cd3          string,   
iss_ins_id_cd4          string,   
mchnt_ins_id_cd1        string,   
mchnt_ins_id_cd2        string,   
mchnt_ins_id_cd3        string,   
mchnt_ins_id_cd4        string,   
term_ins_id_cd1         string,   
term_ins_id_cd2         string,   
term_ins_id_cd3         string,   
term_ins_id_cd4         string,   
term_ins_id_cd5         string,   
acpt_cret_disc_at       string,   
acpt_debt_disc_at       string,   
iss1_cret_disc_at       string,   
iss1_debt_disc_at       string,   
iss2_cret_disc_at       string,   
iss2_debt_disc_at       string,   
iss3_cret_disc_at       string,   
iss3_debt_disc_at       string,   
iss4_cret_disc_at       string,   
iss4_debt_disc_at       string,   
mchnt1_cret_disc_at     string,                     
mchnt1_debt_disc_at     string,   
mchnt2_cret_disc_at     string,   
mchnt2_debt_disc_at     string,   
mchnt3_cret_disc_at     string,   
mchnt3_debt_disc_at     string,   
mchnt4_cret_disc_at     string,   
mchnt4_debt_disc_at     string,   
term1_cret_disc_at      string,   
term1_debt_disc_at      string,   
term2_cret_disc_at      string,   
term2_debt_disc_at      string,   
term3_cret_disc_at      string,   
term3_debt_disc_at      string,   
term4_cret_disc_at      string,   
term4_debt_disc_at      string,   
term5_cret_disc_at      string,   
term5_debt_disc_at      string,   
pay_in                  string,   
exp_id                  string,   
vou_in                  string,   
orig_log_cd             string,   
related_log_cd          string,   
rec_upd_ts              string,   
rec_crt_ts              string,   
trans_media             string,   
pdate                   string                             
);
 
INSERT OVERWRITE TABLE tele_fraud_1011
select t1.* from tbl_common_his_trans t1
left semi join fraudCards_md5 t2
on t1.tfr_in_acct_no=t2.card
where t1.pdate>='20161001' and t1.pdate<='20161131'
union all
select t1.* from tbl_common_his_trans t1
left semi join fraudCards_md5 t2
on t1.tfr_out_acct_no=t2.card
where t1.pdate>='20161001' and t1.pdate<='20161131'
union all
select t1.* from tbl_common_his_trans t1
left semi join fraudCards_md5 t2
on t1.pri_acct_no_conv=t2.card
where t1.pdate>='20161001' and t1.pdate<='20161131';




select count(*) from tele_fraud;
6873
select count(*) from tele_fraud where pdate>='20150601' and pdate<='20160931';

select count(*) from tele_fraud where pdate>='20160701' and pdate<='20160931';
6021


select count(case when trans_id='S33' then 1 else null end) from tele_fraud
where pdate>='20160101' and pdate<='20160931';

'2015年整个下半年只有8笔         '20160101' and pdate<='20160931';  557
pdate>='20160701' and pdate<='20160931';   547笔

select count(case when trans_id='S25' then 1 else null end) from tele_fraud
where pdate>='20160701' and pdate<='20160931';

所有913笔    
where pdate>='20160601' and pdate<='20160931'; 897笔
where pdate>='20160701' and pdate<='20160931'; 825笔

select count(case when trans_id='S24' then 1 else null end) from tele_fraud
where pdate>='20160101' and pdate<='20160931';
pdate>='20150601' and pdate<='20160931'; 2141
pdate>='20150601' and pdate<='20151231'; 12
pdate>='20160101' and pdate<='20160931'; 2129

 
select * from tele_fraud
where trans_id='S33' and pdate>='20150601' and pdate<='20160601';   啥都没有

select * from tele_fraud
where trans_id='S25' and pdate>='20150601' and pdate<='20160601';   啥都没有

select * from tele_fraud
where trans_id='S24' and pdate>='20150601' and pdate<='20160601';   有好多

select count(*) from tele_fraud
where pdate>='20150601' and pdate<='20160601';   246


select count(case when trans_id='S24' then null else 1 end) from tele_fraud
where pdate>='20150601' and pdate<='20160601'; 

pdate>='20150601' and pdate<='20160601' 期间没有发生任何转账交易,发生过203笔取现  

select count(case when trans_id='S24' then null else 1 end) from tele_fraud where pdate>='20150601' and pdate<='20160601';  43
 
 

INSERT OVERWRITE TABLE tele_fraud_pre
select * from tele_fraud
where trans_id<>'S24' and pdate>='20150601' and pdate<='20160601';



select count(trans_id) from tele_fraud_pre group by trans_id;

S10	14  预授权
S17	1   预付费卡账户验证                        
S22	21  消费
S30	1   退货
S35	5   预授权完成
V40	1   预授权撤销

select trans_id, count(trans_id) from tele_fraud
where pdate>='20160601' and pdate<='20160931' group by trans_id;

E00     9  差错-查询
E02     9   差错-查复      
E04     10  差错-调单  
E05     10  差错-调单回复 
E23     5   差错-结算的退单
E73     1   差错例外-贷(发卡方) 
I01     1   基于PBOC借/贷记标准脚本处理结果通知     
R03     13  圈存冲正
R24     1   取现冲正
S00     1597  余额查询 
S03     835  圈存
S10     4  预授权 
S17     359  预付费卡账户验证  
S20     1  预授权完成
S22     219  消费 
S24     1939  取现 
S25     897    转账转出 
S33     557    转账转入
S35     1    预授权完成
V52     1    消费撤消


create table if not exists test_quxian(card string, time string, trans_md string, 
cross_dist_in string, ATM string, fwd_settle_at string);


INSERT OVERWRITE TABLE test_quxian
select pri_acct_no_conv, concat(pdate, loc_trans_tm) as time, trans_md,
cross_dist_in, concat(mchnt_cd,term_id), fwd_settle_at from tele_fraud
where trans_id='S24' and pdate>='20160601' and pdate<='20160931'
order by pri_acct_no_conv,time;

TRUNCATE TABLE test_quxian;


create table if not exists test_zhuanzhang(tfr_in_acct_no string, tfr_out_acct_no string, trans_at string,
time string, acpt_ins_id_cd string, trans_md string); 

INSERT OVERWRITE TABLE test_zhuanzhang
select tfr_in_acct_no, tfr_out_acct_no, concat(pdate, loc_trans_tm) as time, substr(acpt_ins_id_cd,5,4) as loc, trans_md, trans_at
 from tele_fraud
where trans_id='S25' or trans_id='S33' and pdate>='20160601' and pdate<='20160931'
order by tfr_in_acct_no,time,tfr_out_acct_no;
 


 
 
INSERT OVERWRITE TABLE tele_fraud_0709
select t1.* from tbl_common_his_trans t1
left semi join fraudCards_md5 t2
on t1.tfr_in_acct_no=t2.card
where t1.pdate>='20160701' and t1.pdate<='20160931'
union all
select t1.* from tbl_common_his_trans t1
left semi join fraudCards_md5 t2
on t1.tfr_out_acct_no=t2.card
where t1.pdate>='20160701' and t1.pdate<='20160931'
union all
select t1.* from tbl_common_his_trans t1
left semi join fraudCards_md5 t2
on t1.pri_acct_no_conv=t2.card
where t1.pdate>='20160701' and t1.pdate<='20160931';

 

CREATE TABLE tele_fraud_1506_1609 AS SELECT * FROM tele_fraud;



INSERT OVERWRITE TABLE tele_fraud
select * from tele_fraud_0709
union all
select * from tele_fraud_1011;



alter table tele_fraud_1011 change trans_id trans_id double;  


"






扩展

hive -e "set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.created.files = 1000000; 
set mapred.max.split.size=10240000000;
set mapred.min.split.size.per.node=10240000000;
set mapred.min.split.size.per.rack=10240000000;
set mapreduce.jobtracker.split.metainfo.maxsize = -1;
set mapreduce.job.queuename=root.queue2;

create table if not exists tele_InOutRound2(tfr_in_acct_no string, tfr_out_acct_no string, trans_at string);
insert overwrite table tele_InOutRound2
select t1.tfr_in_acct_no, t1.tfr_out_acct_no, t1.trans_at
from tbl_common_his_trans t1
left semi join sus1_cards t2
on t1.tfr_in_acct_no = t2.card
where t1.trans_id='S33' and t1.pdate>='20160701' and t1.pdate<='20161131'
union all
select t1.tfr_in_acct_no, t1.tfr_out_acct_no, t1.trans_at
from tbl_common_his_trans t1
left semi join sus1_cards t2
on t1.tfr_out_acct_no = t2.card
where t1.trans_id='S33' and t1.pdate>='20160701' and t1.pdate<='20161131';


create table if not exists tele_staticRound2(
tfr_in_acct_no string,
tfr_out_acct_no string,
amount string)
ROW FORMAT DELIMITED FIELDS
TERMINATED BY '\t';

insert overwrite table tele_staticRound2
select tfr_in_acct_no,tfr_out_acct_no, sum(trans_at) as amount
from tele_InOutRound2
group by tfr_in_acct_no,tfr_out_acct_no;



create table if not exists sus2_cards(card string);
insert overwrite table sus2_cards
select distinct card from(
select tfr_in_acct_no as card from tele_InOutRound2
union all
select tfr_out_acct_no as card from tele_InOutRound2
)temp;"