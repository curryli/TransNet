package MultiEvent

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
 
import SparkContext._ 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import AlgorithmUtil._
import scala.reflect.ClassTag

object MultiUtil {
  
   private val startDate = "20160707"
   private val endDate = "20160707"
   
   
  class VertexProperty()
   class EdgePropery()
   
   case class card_VP(
     val  card: String
     ) extends VertexProperty
     
   case class region_VP(
     val  region: String
     ) extends VertexProperty
     
   case class mchnt_cd_VP(
     val  mchnt_cd: String
     ) extends VertexProperty
     
   case class term_id_VP(
     val  term_id: String
     ) extends VertexProperty
   
   case class Vertex_Property_Class(
     val vertexType: String,
     val card_VP: card_VP,
     val region_VP: region_VP,
     val mchnt_cd_VP: mchnt_cd_VP,
     val term_id_VP: term_id_VP
    ) extends VertexProperty    
        
   case class trans_to_EP(
     val money: Long,
     val date: String,
     val charge: Long,
     val ForeignCount: Int,
     val nightCount:Int,
     val count: Int
     )
     
    case class trans_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long,
     val count: Int
     )
     
    case class quxian_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long,
     val count: Int
     ) 
     
    case class query_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long,
     val count: Int
     )
     
    case class consume_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long,
     val count: Int
     )  
     
    case class Edge_Property_Class(
     val edgeType: String,
     val trans_to_EP: trans_to_EP,
     val trans_at_EP: trans_at_EP,
     val quxian_at_EP: quxian_at_EP,
     val query_at_EP: query_at_EP,
     val consume_at_EP: consume_at_EP
    ) extends EdgePropery  
     
    
  def getFromSQL(hc: HiveContext) = {  
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.queue2")

    val transdata = hc.sql(
      s"select tfr_in_acct_no," +
      s"tfr_out_acct_no, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
      s"trim(trans_md), " +
      s"mchnt_tp, " +
      s"mchnt_cd, " +
      s"card_accptr_nm_addr, " +
      s"term_id, "+
      s"total_disc_at " +        //手续
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=$startDate and hp_settle_dt<=$endDate and trans_id ='S33' ")
        .toDF("srccard","dstcard","money","date","loc_trans_tm","region_cd","trans_md","mchnt_tp","mchnt_cd","fkh", "term_id", "total_disc_at")
        .repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER)  
    //transdata.show(5)
         
    val quxiandata = hc.sql(
      s"select pri_acct_no_conv, "+
      s"fwd_settle_at, "+
      s"hp_settle_dt, "+
      s"loc_trans_tm, "+
      s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
      s"trim(trans_md), " +
      s"card_accptr_nm_addr, " +
      s"term_id, "+
      s"total_disc_at " +        //手续费
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=$startDate and hp_settle_dt<=$endDate and trans_id ='S24' ")
        .toDF("card","money","date","loc_trans_tm","region_cd","trans_md","fkh", "term_id", "total_disc_at")
        .repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER)  
     // quxiandata.show(5)
      
      
    val querydata = hc.sql(
      s"select pri_acct_no_conv, " +
      s"hp_settle_dt, "+
      s"loc_trans_tm, "+
      s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
      s"trim(trans_md), " +
      s"card_accptr_nm_addr, " +
      s"term_id, " +
      s"total_disc_at " +        //手续费
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=$startDate and hp_settle_dt<=$endDate and trans_id ='S00' ")
        .toDF("card","date","loc_trans_tm","region_cd","trans_md","fkh", "term_id", "total_disc_at")
        .repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER)  
      //querydata.show(5)
       
    val consumedata = hc.sql(
      s"select pri_acct_no_conv, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
      s"trim(trans_md), " +
      s"mchnt_tp, " +
      s"mchnt_cd, " +
      s"card_accptr_nm_addr, " +
      s"term_id, "+
      s"total_disc_at " +        //手续费
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=$startDate and hp_settle_dt<=$endDate and trans_id ='S22' ")
        .toDF("card","money","date","loc_trans_tm","region_cd","trans_md","mchnt_tp","mchnt_cd","fkh","term_id","total_disc_at")
        .repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER)  
     //consumedata.show(5)
    
    
     List(transdata, quxiandata, querydata, consumedata)
  }
}