package WireFraud

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel


object StaticInOut {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("StaticInOut") 
    val sc = new SparkContext(conf)
     
    
    val textfile = sc.textFile(args(0)).persist(StorageLevel.MEMORY_AND_DISK_SER)       //xrli/testfile.txt   xrli/HiveTrans03
    
    
    // 读入时指定编码  
    val InPairRdd = textfile.map(line => (line.split("\\001")(1).toLong, line.split("\\001")(0)))                //.map(item => item(0))         
    val OutPairRdd = textfile.map(line => ( line.split("\\001")(3).toLong, line.split("\\001")(2)))
    
    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
     
    val edgeRDD = textfile.map { line=>
        val lineArray = line.split("\\001")

        val srcId = lineArray(3).toLong
        val dstId = lineArray(1).toLong
        val msum = lineArray(4).toDouble
        val csum = lineArray(5).toInt
        val locsum = lineArray(6).toInt
        val foreignsum = lineArray(7).toInt
        Edge(srcId, dstId, (msum,csum,locsum,foreignsum))
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系    
    //graph.persist(StorageLevel.MEMORY_AND_DISK_SER)  报错    java.lang.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level
    val graph = Graph(verticeRDD, edgeRDD)
     
    val SumInVRDD = graph.aggregateMessages[(Double,Int,Int,Int)](
      triplet => {   
          triplet.sendToDst(triplet.attr);         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数,总异地转出次数)，所以不管是转出金额还是转入金额我们都累加起来
     },
      (a, b) =>  (a._1 + b._1,  a._2 + b._2, a._4 + b._4, 0)
    )
    
    val SumOutVRDD = graph.aggregateMessages[(Double,Int,Int,Int)](
      triplet => {   
          triplet.sendToSrc(triplet.attr)         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数，,总异地转入次数)，所以不管是转出金额还是转入金额我们都累加起来
     },
      (a, b) =>  (a._1 + b._1,  a._2 + b._2, a._4 + b._4, 0)
    )
    
    
     val SumInGraph = graph.outerJoinVertices(SumInVRDD){
      (vid, card, q) => (card, q.getOrElse((0.0,0,0,0)))
     }
    
    val SumInOutGraph = SumInGraph.outerJoinVertices(SumOutVRDD){
      (vid, p, q) => (p._1, p._2._1,p._2._2, q.getOrElse(0.0,0,0,0)._1, q.getOrElse(0.0,0,0,0)._2,
          p._2._1+q.getOrElse(0.0,0,0,0)._1, p._2._2+q.getOrElse(0.0,0,0,0)._2, math.abs( p._2._1-q.getOrElse(0.0,0,0,0)._1),
          math.abs(p._2._1-q.getOrElse(0.0,0,0,0)._1)/(p._2._1+q.getOrElse(0.0,0,0,0)._1), p._2._3+q.getOrElse(0.0,0,0,0)._3   
      )
     }
    //card, 金额in，次数in，金额out， 次数out， 总金额，总次数， 金额净差, 净总比,总异地笔数
    
    //SumInOutGraph.vertices.collect().foreach(println)
    
    
     val SortedAmountIn = SumInOutGraph.vertices.sortBy(_._2._2, false)
     SortedAmountIn.saveAsTextFile("TeleTrans/InOut/SortedAmountIn")   // 
    
     val SortedAmountOut= SumInOutGraph.vertices.sortBy(_._2._4, false)
     SortedAmountOut.saveAsTextFile("TeleTrans/InOut/SortedAmountOut")   // 
     
     val SortedAmount = SumInOutGraph.vertices.sortBy(_._2._6, false)
     SortedAmount.saveAsTextFile("TeleTrans/InOut/SortedAmount")   // 
    
     
     val SortedCountIn = SumInOutGraph.vertices.sortBy(_._2._3, false)
     SortedCountIn.saveAsTextFile("TeleTrans/InOut/SortedCountIn")   // 
    
     val SortedCountOut= SumInOutGraph.vertices.sortBy(_._2._5, false)
     SortedCountOut.saveAsTextFile("TeleTrans/InOut/SortedCountOut")   // 
     
     val SortedCount = SumInOutGraph.vertices.sortBy(_._2._7, false)
     SortedCount.saveAsTextFile("TeleTrans/InOut/SortedCount")   // 
     
     val SortedExp = SumInOutGraph.vertices.sortBy(_._2._8, false)
     SortedExp.saveAsTextFile("TeleTrans/InOut/SortedExp")   // 
	 
	   val SortedRatio = SumInOutGraph.vertices.sortBy(_._2._9, false)
     SortedRatio.saveAsTextFile("TeleTrans/InOut/SortedRatio")   // 
     
     val SortedForeign = SumInOutGraph.vertices.sortBy(_._2._10, false)
     SortedForeign.saveAsTextFile("TeleTrans/InOut/SortedForeign")   // 

    
     
     
     
     
     
     val inDegrees: VertexRDD[Int] = graph.inDegrees
    
    val DegInGraph = graph.outerJoinVertices(graph.inDegrees){
      (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0))}
    
//相当于 SELECT v.id, v1.attr, v2.outDegree from vertices v1
//OUT JOIN (
//  select id, count(outDegrees) as outDegree from vertices
//) v2 ON v1.id = v2.id
    
    val DegInOutGraph = DegInGraph.outerJoinVertices(graph.outDegrees){
      (vid, p, outDegOpt) => (p._1, p._2, outDegOpt.getOrElse(0))}
    
// (id,(卡号,入度,出度))    
    
    
//  val maxDegIn = sc.parallelize(DegInOutGraph.vertices.sortBy(_._2._2, false).take(100000)).saveAsTextFile("xrli/TransNet/maxDegIn")   //按入度排序
    val SortedDegOut = DegInOutGraph.vertices.sortBy(_._2._3, false)
    SortedDegOut.saveAsTextFile("TeleTrans/InOut/SortedDegOut")   //按出度排序
    
    val SortedDegIn = DegInOutGraph.vertices.sortBy(_._2._2, false)
    SortedDegIn.saveAsTextFile("TeleTrans/InOut/SortedDegIn")  

    val MaxInOut = sc.parallelize(SortedDegOut.take(100000)).intersection(sc.parallelize(SortedDegIn.take(100000)))
    
    MaxInOut.saveAsTextFile("TeleTrans/InOut/MaxInOut")
    
     
    sc.stop()
  }
  
   
}