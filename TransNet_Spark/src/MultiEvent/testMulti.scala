package MultiEvent
//有问题  也不要用
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
import org.apache.spark.rdd.RDD

object testMulti  { 
   private val startDate = "20160707"
   private val endDate = "20160707"
   
   class VertexProperty()// extends Serializable  
   class EdgePropery()// extends Serializable 
   
   
   case class card_VP [+T] (
     val  card: String
     ) extends VertexProperty
     
   case class region_VP(
     val  region: String
     ) extends VertexProperty
     
  
 
     
  def main(args: Array[String]) { 
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.INFO);
    Logger.getLogger("akka").setLevel(Level.INFO);
    Logger.getLogger("hive").setLevel(Level.INFO);
    Logger.getLogger("parse").setLevel(Level.INFO);
    
    val sparkConf = new SparkConf().setAppName("MultiCompute")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)  
  
    val startTime = System.currentTimeMillis(); 
    
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
      //quxiandata.show(5)
      
       
      
    println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    
    
    import sqlContext.implicits._   
      val InPairRdd = transdata.map(line => line.getString(0))                
      val OutPairRdd = transdata.map(line => line.getString(1)) 
      val QXcardRdd = quxiandata.map(line => line.getString(0)) 
  
      
    val cardRdd = InPairRdd.union(OutPairRdd).union(QXcardRdd).distinct().map{line => 
      val card = new card_VP(line.trim)
      val vertexType = "card"
      (HashEncode.HashMD5(line.trim), (vertexType, card))
      }
    
    val transregionRdd = transdata.map(line => line.getString(5)) 
    val QXregionRdd = quxiandata.map(line => line.getString(4)) 
     
    val regionRdd = transregionRdd.union(QXregionRdd).distinct().map{line => 
      val region = new region_VP(line.trim)
      val vertexType = "region"
      (HashEncode.HashMD5(line.trim), (vertexType, region))
      }
   
  
    var graph: Graph[VertexProperty, String] = null
    
    val vertexArray = Array(
      (1L, new card_VP("card1")),
      (2L, new card_VP("card2")),
      (3L, new region_VP("reg1")),
      (4L, new region_VP("reg2"))
    )
    val vertexRDD = sc.parallelize(vertexArray)
     
    
    //RDD不支持    协变与逆变 ？ 
  // var verticeRDD: RDD[(Long, (String, testMulti.VertexProperty))] = cardRdd
    
    
    println("verticeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    

     
  
    
    sc.stop()
    
  } 
  
} 
 