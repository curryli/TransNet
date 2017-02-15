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

object MultiCompute { 
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
    
   case class trans_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long) extends EdgePropery
     
    case class trans_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long) extends EdgePropery
     
    case class quxian_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long) extends EdgePropery
     
    case class query_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long) extends EdgePropery
     
    case class consume_at_EP(
     val money: Long,
     val date: String,
     val loc_trans_tm: String,
     val trans_md: String,
     val total_disc_at: Long) extends EdgePropery
   
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
    transdata.show(5)
         
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
      quxiandata.show(5)
      
      
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
      querydata.show(5)
      
      println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
      
      
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
    consumedata.show(5)
      
    println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    
    
    import sqlContext.implicits._   
      val InPairRdd = transdata.map(line => line.getString(0))                
      val OutPairRdd = transdata.map(line => line.getString(1)) 
      val QXcardRdd = quxiandata.map(line => line.getString(0)) 
      val QYcardRdd = querydata.map(line => line.getString(0))
      val consumeRdd = consumedata.map(line => line.getString(0))
    
    
    val cardRdd = InPairRdd.union(OutPairRdd).union(QXcardRdd).union(QYcardRdd).union(consumeRdd).distinct().map{line => 
      val card = new card_VP(line.trim)
      val region = new region_VP("")
      val mchnt_cd = new mchnt_cd_VP("")
      val term_id = new term_id_VP("")
      val vertexType = "card"
      (HashEncode.HashMD5(line.trim), (vertexType, card, region, mchnt_cd, term_id))
      }
    
    val transregionRdd = transdata.map(line => line.getString(5)) 
    val QXregionRdd = quxiandata.map(line => line.getString(4)) 
    val QYregionRdd = querydata.map(line => line.getString(3)) 
    val consume_regionRdd = consumedata.map(line => line.getString(4))
    val regionRdd = transregionRdd.union(QXregionRdd).union(QYregionRdd).union(consume_regionRdd).distinct().map{line => 
      val card = new card_VP("")
      val region = new region_VP(line.trim)
      val mchnt_cd = new mchnt_cd_VP("")
      val term_id = new term_id_VP("")
      val vertexType = "region"
      (HashEncode.HashMD5(line.trim), (vertexType, card, region, mchnt_cd, term_id))
      }
   
    val transmchntRdd = transdata.map(line => line.getString(8)) 
    val consume_mchntRdd = consumedata.map(line => line.getString(7))
    val mchntRdd = transmchntRdd.union(consume_mchntRdd).distinct().map{line => 
      val card = new card_VP("")
      val region = new region_VP("")
      val mchnt_cd = new mchnt_cd_VP(line.trim)
      val term_id = new term_id_VP("")
      val vertexType = "mchnt_cd"
      (HashEncode.HashMD5(line.trim), (vertexType, card, region, mchnt_cd, term_id))
      }
     
   
    val QXtermRdd = quxiandata.map(line => line.getString(7)) 
    val QYtermRdd = querydata.map(line => line.getString(6)) 
    val consume_termRdd = consumedata.map(line => line.getString(9)) 
    val termRdd = QXtermRdd.union(QYtermRdd).union(consume_termRdd).distinct().map{line => 
      val card = new card_VP("")
      val region = new region_VP("")
      val mchnt_cd = new mchnt_cd_VP("")
      val term_id = new term_id_VP(line.trim)
      val vertexType = "term_id"
      (HashEncode.HashMD5(line.trim), (vertexType, card, region, mchnt_cd, term_id))
      }
    
    val verticeRDD = cardRdd.union(regionRdd).union(mchntRdd).union(termRdd)
    
    println("verticeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    

    val transRDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(1))
        val trans_EP = new trans_EP(line.getLong(2),line.getString(3),line.getString(4),line.getString(6),line.getLong(11))
        val trans_at_EP = new trans_at_EP(0L,"","","",0L)
        val quxian_at_EP = new quxian_at_EP(0L,"","","",0L)
        val query_at_EP =  new query_at_EP(0L,"","","",0L)
        val consume_at_EP = new consume_at_EP(0L,"","","",0L) 
        val edgeType = "trans_to"
        Edge(srcId, dstId, (edgeType,trans_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP))
    }
    
    
    val trans_at_RDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(5))
        val trans_EP = new trans_EP(0L,"","","",0L)
        val trans_at_EP = new trans_at_EP(line.getLong(2),line.getString(3),line.getString(4),line.getString(6),line.getLong(11))
        val quxian_at_EP = new quxian_at_EP(0L,"","","",0L)
        val query_at_EP =  new query_at_EP(0L,"","","",0L)
        val consume_at_EP = new consume_at_EP(0L,"","","",0L) 
        val edgeType = "trans_at"
        Edge(srcId, dstId, (edgeType,trans_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP))
    }
    
    val quxian_at_RDD = quxiandata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(4))
        val trans_EP = new trans_EP(0L,"","","",0L)
        val trans_at_EP = new trans_at_EP(0L,"","","",0L)
        val quxian_at_EP = new quxian_at_EP(line.getLong(1),line.getString(2),line.getString(3),line.getString(5),line.getLong(8))
        val query_at_EP =  new query_at_EP(0L,"","","",0L)
        val consume_at_EP = new consume_at_EP(0L,"","","",0L) 
        val edgeType = "quxian_at"
        Edge(srcId, dstId, (edgeType,trans_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP))
    }
    
    val query_at_RDD = querydata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(3))
        val trans_EP = new trans_EP(0L,"","","",0L)
        val trans_at_EP = new trans_at_EP(0L,"","","",0L)
        val quxian_at_EP = new quxian_at_EP(0L,"","","",0L)
        val query_at_EP =  new query_at_EP(0L,line.getString(1),line.getString(2),line.getString(4),line.getLong(7))
        val consume_at_EP = new consume_at_EP(0L,"","","",0L) 
        val edgeType = "query_at"
        Edge(srcId, dstId, (edgeType,trans_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP))
    }
    
       
    val consume_at_RDD = consumedata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(4))
        val trans_EP = new trans_EP(0L,"","","",0L)
        val trans_at_EP = new trans_at_EP(0L,"","","",0L)
        val quxian_at_EP = new quxian_at_EP(0L,"","","",0L)
        val query_at_EP =  new query_at_EP(0L,"","","",0L)
        val consume_at_EP = new consume_at_EP(line.getLong(1),line.getString(2),line.getString(3),line.getString(5),line.getLong(10))
        val edgeType = "consume_at"
        Edge(srcId, dstId, (edgeType,trans_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP))
    }
    
 
    
    val edgeRDD = transRDD.union(trans_at_RDD).union(quxian_at_RDD).union(query_at_RDD).union(consume_at_RDD)
 
    println("edgeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
     
    transdata.unpersist(blocking=false)
    quxiandata.unpersist(blocking=false)
    querydata.unpersist(blocking=false)
    consumedata.unpersist(blocking=false)
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
 
    println("card vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("card")).take(5).foreach(println)
    println("region vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("region")).take(5).foreach(println)
    println("mchnt_cd vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("mchnt_cd")).take(5).foreach(println)
    println("term_id vertices:")
    graph.vertices.filter(pred=> pred._2._1.equals("term_id")).take(5).foreach(println)
     
    println("trans edges:")
    graph.edges.filter(f=>f.attr._1.equals("trans_to")).take(5).foreach(println)
    println("trans_at edges:")
    graph.edges.filter(f=>f.attr._1.equals("trans_at")).take(5).foreach(println)
    println("quxian_at edges:")
    graph.edges.filter(f=>f.attr._1.equals("quxian_at")).take(5).foreach(println)
    println("query_at edges:")
    graph.edges.filter(f=>f.attr._1.equals("query_at")).take(5).foreach(println) 
    println("consume_at edges:")
    graph.edges.filter(f=>f.attr._1.equals("consume_at")).take(5).foreach(println) 
    
//card vertices:
//(895004484848200,(card,card_VP(f6dcd82d211c45964f11ea5e1ebd12fa),region_VP(),mchnt_cd_VP(),term_id_VP()))
//
//region vertices:
//(554213000667325,(region,card_VP(),region_VP(1710),mchnt_cd_VP(),term_id_VP()))
//
//mchnt_cd vertices:
//(759701945336593,(mchnt_cd,card_VP(),region_VP(),mchnt_cd_VP(898440458120183),term_id_VP()))
//
//term_id vertices:
//(869297802268839,(term_id,card_VP(),region_VP(),mchnt_cd_VP(),term_id_VP(13213491)))
//
//trans edges:
//Edge(366931893679,591504185106010,(trans_to,trans_EP(500000,20160707,174125,2,300),trans_at_EP(0,,,,0),quxian_at_EP(0,,,,0),query_at_EP(0,,,,0),consume_at_EP(0,,,,0)))
//
//trans_at edges:
//Edge(3848059368651,911711054742209,(trans_at,trans_EP(0,,,,0),trans_at_EP(1000000,20160707,192935,2,300),quxian_at_EP(0,,,,0),query_at_EP(0,,,,0),consume_at_EP(0,,,,0)))
//
//quxian_at edges:
//Edge(244901529203,207958152209094,(quxian_at,trans_EP(0,,,,0),trans_at_EP(0,,,,0),quxian_at_EP(200000,20160707,101508,2,360),query_at_EP(0,,,,0),consume_at_EP(0,,,,0)))
//
//query_at edges:
//Edge(220903107022,367616193047495,(query_at,trans_EP(0,,,,0),trans_at_EP(0,,,,0),quxian_at_EP(0,,,,0),query_at_EP(0,20160707,190621,2,0),consume_at_EP(0,,,,0)))
//
//consume_at edges:
//Edge(64119533635,943164737173323,(consume_at,trans_EP(0,,,,0),trans_at_EP(0,,,,0),quxian_at_EP(0,,,,0),query_at_EP(0,,,,0),consume_at_EP(20000,20160707,204253,2,76)))

    sc.stop()
    
  } 
  
} 

