package FraudFlow
import org.apache.spark._ 
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import SparkContext._ 
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext

object sparkt1SQL { 
  def main(args: Array[String]) { 
     //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val sparkConf = new SparkConf().setAppName("sparkSQL")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.queue2")

    hc.sql("CREATE DATABASE IF NOT EXISTS TeleFraud")
    hc.sql("USE TeleFraud")
    hc.sql(s"CREATE TABLE IF NOT EXISTS tele_trans0701(" + 
                    s"tfr_in_acct_no string," + 
                    s"tfr_out_acct_no string," + 
                    s"fwd_settle_at double," + 
                    s"hp_settle_dt string," + 
                    s"loc_trans_tm string," + 
                    s"acpt_ins_id_cd string," + 
                    s"trans_md string," + 
                    s"cross_dist_in string)")
                    
    
                    
   val da = hc.sql(
      s"select tfr_in_acct_no," +
      s"tfr_out_acct_no, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"acpt_ins_id_cd, " +
      s"trans_md, " +
      s"cross_dist_in " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt=20160701 and " +
      s"trans_id ='S33' ").repartition(400)

        
     val transdata = hc.sql(s"select tfr_in_acct_no, tfr_out_acct_no, sum(fwd_settle_at) as amount " +
        "from tele_trans0701 " +
        "group by tfr_in_acct_no,tfr_out_acct_no").repartition(100).toDF()
        
  
//     import hc.implicits._
//     val InPairRdd = transdata.map(line => (line.getLong(0), line.getString(1)) )       
//     val OutPairRdd = transdata.map(line => (line.getLong(2), line.getString(3)))      
//     val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
  } 
}
