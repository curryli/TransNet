package FraudFlow
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
 
import SparkContext._ 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel


//case class transProperty(val srccard: String, val dstcard: String, val money: Double, val date: String, val time: String, 
//                           val acpt_ins_id_cd: String,  val trans_md: String,  val cross_dist_in: String)

object testGraphxSQL { 
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

    val data = hc.sql(
      s"select tfr_in_acct_no," +
      s"tfr_out_acct_no, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"acpt_ins_id_cd, " +
      s"trans_md, " +
      s"cross_dist_in " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=20160701 and hp_settle_dt<=20160701 and trans_id ='S33' ").repartition(100)
        .toDF("srccard","dstcard","money","date","time","acpt_ins_id_cd","trans_md","cross_dist_in")
        
 
    val transdata = data.groupBy("srccard","dstcard").sum("money")
    println("transdata  column nums: " + transdata.columns.size)
      
      
    val InPairRdd = transdata.map(line => (BKDRHash(line.getString(0)), line.getString(0)))                
    val OutPairRdd = transdata.map(line => (BKDRHash(line.getString(1)), line.getString(1)))      
    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
    
    println(verticeRDD.count())
    
    val edgeRDD = transdata.map { line=>
        val srcId = BKDRHash(line.getString(0))
        val dstId = BKDRHash(line.getString(1))
        val money = line.getLong(2)
        Edge(srcId, dstId, money)
    }
    
  
    val graph = Graph(verticeRDD, edgeRDD)
    
    val degGraph = graph.outerJoinVertices(graph.degrees){
      (vid, name, DegOpt) => (name, DegOpt.getOrElse(0))
    }
     
     //去除边出入度和为2的图
    var newgraph = degGraph.subgraph(vpred => (vpred.srcAttr._2 + vpred.srcAttr._2) > 2)
    
    
    //val sgraph = graph.connectedComponents()
    val sgraph = newgraph.stronglyConnectedComponents(2)
    println("vertice1:" + sgraph.vertices.first()._1 + "Belong to componet: " + sgraph.vertices.first()._2)
    
    
    
    
    
    
  } 
  
  
  
  def BKDRHash( str:String) :Long ={
   val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
   var hash:Long  = 0
   for(i <- 0 to str.length-1){
    hash = hash * seed + str.charAt(i)
    hash = hash.&("137438953471".toLong)        //0x1FFFFFFFFF              //固定一下长度
   }
   return hash 
}
  
} 