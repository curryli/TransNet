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
import AlgorithmUtil._

object StaticForConnected { 
  def main(args: Array[String]) { 
     //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);
    
    val sparkConf = new SparkConf().setAppName("transGraph")
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

    val startDate = "20160707"
    val endDate = "20160707"
           
    val data = hc.sql(
      s"select tfr_in_acct_no," +
      s"tfr_out_acct_no, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
      s"trans_md, " +
      s"mchnt_tp, " +
      s"mchnt_cd, " +
      s"card_accptr_nm_addr, " +
      s"cross_dist_in " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=$startDate and hp_settle_dt<=$endDate and trans_id ='S33' ")
        .toDF("srccard","dstcard","money","date","time","region_cd","trans_md","mchnt_tp","mchnt_cd","card_accptr_nm_addr","cross_dist_in")
        .repartition(100)
  
    println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
        
    import sqlContext.implicits._   
    val InPairRdd = data.map(line => (BKDRHash(line.getString(0)), line.getString(0)))                
    val OutPairRdd = data.map(line => (BKDRHash(line.getString(1)), line.getString(1)))      
    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
    
    //println(verticeRDD.count())
    
    val edgeRDD = data.map { line=>
        val srcId = BKDRHash(line.getString(0))
        val dstId = BKDRHash(line.getString(1))
        val money = line.getLong(2)
        Edge(srcId, dstId, (money, 1))
    }
    
    var graph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.EdgePartition2D)
    
    graph = graph.groupEdges((ea,eb) => (ea._1+eb._1, ea._2+eb._2))
    
    val tempDegGraph = graph.outerJoinVertices(graph.degrees){
      (vid, encard, DegOpt) => (encard, DegOpt.getOrElse(0))
    }
     
     //去除边出入度和为2的图
    var coregraph = tempDegGraph.subgraph(epred = triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2) 
 
    coregraph = coregraph.outerJoinVertices(coregraph.degrees){
       (vid, tempProperty, degOpt) => (tempProperty._1, degOpt.getOrElse(0))
    } 
    
    //去除度为0的点
    val degGraph = coregraph.subgraph(vpred = (vid, property) => property._2!=0).cache
    
    val cgraph = ConnectedComponents.run(degGraph)
    println("create cgraph in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    //var cgraph = degGraph.connectedComponents()
    
    
    val connectedCount = cgraph.vertices.map(pair=>(pair._2, 1)).reduceByKey(_+_).sortBy(f => f._2, true)
    //connectedCount  (团体, 对应团体规模) 
    
    println("create connectedCount done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val ccgraph = degGraph.outerJoinVertices(cgraph.vertices){
      (vid, tempProperty, connectedOpt) => (tempProperty._1, connectedOpt)
    }
    // ccgraph   (vid,顶点名称，对应团体)
    
    val ccVertice = ccgraph.vertices.map(line => (line._1, line._2._1, line._2._2.get))
    
    println("create ccVertice done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val schemacc = StructType(StructField("cc",LongType,true)::StructField("count",IntegerType,true):: Nil)
    val rowRDDcc = connectedCount.map(p=>Row(p._1.toLong, p._2))     //原本p._1  是VertexId类型的, Row  不支持这种类型，所以需要转换成Long型
    val cCDF = sqlContext.createDataFrame(rowRDDcc, schemacc)
    
    val schemacv = StructType(StructField("vid",LongType,true)::StructField("name",StringType,true)::StructField("cc",LongType,true)::Nil)
    val rowRDDcv = ccVertice.map(p=>Row(p._1.toLong,p._2,p._3.toLong))
    val cVDF = sqlContext.createDataFrame(rowRDDcv, schemacv)
   
    //cVDF (vid,顶点名称，对应团体)  join cCDF (对应团体, 团体规模)
    
    println("create dataframe done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    var joinedDF = cVDF.join(cCDF, cVDF("cc") === cCDF("cc"), "left_outer").drop(cVDF("cc"))
    //joinedDF.show()  //(vid,顶点名称，对应团体, 规模) 

    val VidconnectedCount = joinedDF.map(row=>(row.getLong(0), (row.getString(1),row.getLong(2),row.getInt(3))))

    println("create VidconnectedCount done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    VidconnectedCount.take(5).foreach(println)
    
    //VidconnectedCount    RDD(Vid, (顶点名称(即原密卡号)，对应团体, 对应团体规模)) 

    val cCountgraph = coregraph.outerJoinVertices(VidconnectedCount){
      (vid, oldProperty, vccCountprop) => vccCountprop.getOrElse(("0",0L,0))
    }
     // cCountgraph 的顶点属性为  (vid,原密卡号，对应团体, 对应团体规模)
    println("create cCountgraph done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val cckgraph = cCountgraph.outerJoinVertices(KcoresLabel.maxKVertices(cCountgraph,3)){
      (vid, oldProperty, kcores) => (oldProperty._1, oldProperty._2, oldProperty._3, kcores.getOrElse(0)) 
    }
    
    cckgraph.vertices.take(5).foreach(println) 
        
    
    val result = AlgorithmUtil.KcoresLabel.KLabeledVertices(graph, 10, sc)
    result.take(5).foreach(println)
    
    println("All flow done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    sc.stop()
    
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