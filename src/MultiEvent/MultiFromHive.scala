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

object MultiFromHive { 
   private val startDate = "20160707"
   private val endDate = "20160707"
   
  def main(args: Array[String]) { 
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.INFO);
    Logger.getLogger("akka").setLevel(Level.INFO);
    Logger.getLogger("hive").setLevel(Level.INFO);
    Logger.getLogger("parse").setLevel(Level.INFO);
    
    val sparkConf = new SparkConf().setAppName("MultiFromHive")
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
      s"cast(total_disc_at as int) " +        //手续
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
      s"cast(total_disc_at as int) " +        //手续费
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
      s"cast(total_disc_at as int) " +        //手续费
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
      s"cast(total_disc_at as int) " +        //手续费
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
      val card = line.trim
      val region = ""
      val mchnt_cd = ""
      val term_id = ""
      (HashEncode.HashMD5(card), (card, region, mchnt_cd, term_id))
      }
    
    val transregionRdd = transdata.map(line => line.getString(5)) 
    val QXregionRdd = quxiandata.map(line => line.getString(4)) 
    val QYregionRdd = querydata.map(line => line.getString(3)) 
    val consume_regionRdd = consumedata.map(line => line.getString(4))
    val regionRdd = transregionRdd.union(QXregionRdd).union(QYregionRdd).union(consume_regionRdd).distinct().map{line => 
      val card = ""
      val region = line.trim
      val mchnt_cd = ""
      val term_id = ""
      (HashEncode.HashMD5(region), (card, region, mchnt_cd, term_id))
      }   
   
    val transmchntRdd = transdata.map(line => line.getString(8)) 
    val consume_mchntRdd = consumedata.map(line => line.getString(7))
    
    val mchntRdd = transmchntRdd.union(consume_mchntRdd).distinct().map{line => 
      val card = ""
      val region = ""
      val mchnt_cd = line.trim
      val term_id = ""
      (HashEncode.HashMD5(mchnt_cd), (card, region, mchnt_cd, term_id))
      }  
     
   
    val QXtermRdd = quxiandata.map(line => line.getString(7)) 
    val QYtermRdd = querydata.map(line => line.getString(6)) 
    val consume_termRdd = consumedata.map(line => line.getString(9)) 
    val termRdd = QXtermRdd.union(QYtermRdd).union(consume_termRdd).distinct().map{line => 
      val card = ""
      val region = ""
      val mchnt_cd = ""
      val term_id = line.trim
      (HashEncode.HashMD5(term_id), (card, region, mchnt_cd, term_id))
      } 
    
    val verticeRDD = cardRdd.union(regionRdd).union(mchntRdd).union(termRdd)
    
    println("verticeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    

    val transRDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(1))
        val money = line.getLong(2)
        val date = line.getString(3)
        val loc_trans_tm = line.getString(4)
        val trans_md = line.getString(6)
        val total_disc_at = line.getInt(11)
        val edgeType = "trans_to"
        Edge(srcId, dstId, (edgeType,money,date,loc_trans_tm,trans_md,total_disc_at))
    }
    
    val trans_at_RDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(5))
        val money = line.getLong(2)
        val date = line.getString(3)
        val loc_trans_tm = line.getString(4)
        val trans_md = line.getString(6)
        val total_disc_at = line.getInt(11)
        val edgeType = "trans_at"
        Edge(srcId, dstId, (edgeType,money,date,loc_trans_tm,trans_md,total_disc_at))
    }
    
    val quxian_at_RDD = quxiandata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(4))
        val money = line.getLong(1)
        val date = line.getString(2)
        val loc_trans_tm = line.getString(3)
        val trans_md = line.getString(5)
        val total_disc_at = line.getInt(8)
        val edgeType = "quxian_at"
        Edge(srcId, dstId, (edgeType,money,date,loc_trans_tm,trans_md,total_disc_at))
    }
    
    val query_at_RDD = querydata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(3))
        val money = 0L
        val date = line.getString(1)
        val loc_trans_tm = line.getString(2)
        val trans_md = line.getString(4)
        val total_disc_at = line.getInt(7)
        val edgeType = "query_at"
        Edge(srcId, dstId, (edgeType,money,date,loc_trans_tm,trans_md,total_disc_at))
    }
    
    val consume_at_RDD = consumedata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(4))
        val money = line.getLong(1)
        val date = line.getString(2)
        val loc_trans_tm = line.getString(3)
        val trans_md = line.getString(5)
        val total_disc_at = line.getInt(10)
        val edgeType = "consume_at"
        Edge(srcId, dstId, (edgeType,money,date,loc_trans_tm,trans_md,total_disc_at))
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
    graph.vertices.filter(pred=> !pred._2._1.isEmpty()).take(5).foreach(println)
    println("region vertices:")
    graph.vertices.filter(pred=> !pred._2._2.isEmpty()).take(5).foreach(println)
    println("mchnt_cd vertices:")
    graph.vertices.filter(pred=> !pred._2._3.isEmpty()).take(5).foreach(println)
    println("term_id vertices:")
    graph.vertices.filter(pred=> !pred._2._4.isEmpty()).take(5).foreach(println)
     
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
   
    sc.stop()
    
  } 
  
} 




//edgeRDD done in 4 minutes.
//card vertices:
//(895004484848200,(f6dcd82d211c45964f11ea5e1ebd12fa,,,))
//(212218897733187,(8c5c0338f9c803ddfaf861145dbbdba8,,,))

//region vertices:
//(554213000667325,(,1710,,))
//(287698019994952,(,1481,,))

//mchnt_cd vertices:
//(759701945336593,(,,898440458120183,))
//(839989464065591,(,,88739D454110370,))

//term_id vertices:
//(869297802268839,(,,,13213491))
//(107293944451293,(,,,48680388))

//trans edges:
//Edge(366931893679,591504185106010,(trans_to,500000,20160707,174125,2,300))
//Edge(662615384883,184402863167778,(trans_to,9000,20160707,215356,2,300))

//trans_at edges:
//Edge(1038821465723,544297564061083,(trans_at,90000000,20160707,130522,2,800))
//Edge(3002232799122,898410602538832,(trans_at,40000,20160707,094829,2,300))

//quxian_at edges:
//Edge(24754789919,205966948155269,(quxian_at,200000,20160707,144324,2,360))
//Edge(82846437582,367428049914939,(quxian_at,50000,20160707,165156,2,360))

//query_at edges:
//Edge(26796190236,754957898426453,(query_at,0,20160707,163048,2,0))
//Edge(30530953461,367616193047495,(query_at,0,20160707,222657,1,0))

//consume_at edges:
//Edge(1255061718,394500382855935,(consume_at,20000000,20160707,131852,2,8000))
//Edge(5983715379,795224120737932,(consume_at,24450,20160707,204328,1,191))
