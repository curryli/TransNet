package MultiEvent
//很慢，跑不通
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

object MultiNode { 
   private val startDate = "20160707"
   private val endDate = "20160707"
   
   class VertexProperty()// extends Serializable  
   class EdgePropery()// extends Serializable 
   
   
   case class prop_VP[U: ClassTag](
     val prop: U
     ) extends VertexProperty
     
   
   case class prop_EP[U: ClassTag](
     val prop: U
     ) extends EdgePropery
 
     
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
      val card = new prop_VP(line.trim)
      val vertexType = "card"
      (HashEncode.HashMD5(line.trim), (vertexType, card))
      }
    
    val transregionRdd = transdata.map(line => line.getString(5)) 
    val QXregionRdd = quxiandata.map(line => line.getString(4)) 
     
    val regionRdd = transregionRdd.union(QXregionRdd).distinct().map{line => 
      val region = new prop_VP(line.trim)
      val vertexType = "region"
      (HashEncode.HashMD5(line.trim), (vertexType, region))
      }
   
   
    val verticeRDD = cardRdd.union(regionRdd)
    
    
    println("verticeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    

    
    val trans_to_RDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(1))
        val trans_to_EP = new prop_EP(line.getLong(2),line.getString(3),line.getString(4),line.getString(6),line.getLong(11))
        val edgeType = "trans_to"
        Edge(srcId, dstId, (edgeType, trans_to_EP))
    }
     
  
    val trans_at_RDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(5))
        val trans_at_EP = new prop_EP(line.getLong(2),line.getString(3),line.getString(4),line.getString(6),line.getLong(11))
        val edgeType = "trans_at"
        Edge(srcId, dstId, (edgeType, trans_at_EP))
    }
    
    val quxian_at_RDD = quxiandata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(4))
        val quxian_at_EP = new prop_EP(line.getLong(1),line.getString(2),line.getString(3),line.getString(5),line.getLong(8))
        val edgeType = "quxian_at"
        Edge(srcId, dstId, (edgeType, quxian_at_EP))
    }
    
    val edgeRDD = trans_to_RDD.union(trans_at_RDD).union(quxian_at_RDD)
     
     
    transdata.unpersist(blocking=false)
    quxiandata.unpersist(blocking=false)
    
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
    println("graph Edge Num is: " + graph.numEdges)
    
    println("card vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("card")).take(2).foreach(println)
    println("region vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("region")).take(2).foreach(println)
    println("mchnt_cd vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("mchnt_cd")).take(2).foreach(println)
    println("term_id vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("term_id")).take(2).foreach(println)
     
    println("trans_to edges:")
    graph.edges.filter(f=>f.attr._1.equals("trans_to")).take(2).foreach(println)
    println("trans_at edges:")
    graph.edges.filter(f=>f.attr._1.equals("trans_at")).take(2).foreach(println)
    println("quxian_at edges:")
    graph.edges.filter(f=>f.attr._1.equals("quxian_at")).take(2).foreach(println)

    
    
    sc.stop()
    
  } 
  
} 
 