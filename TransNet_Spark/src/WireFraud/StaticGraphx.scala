package WireFraud

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel


object StaticGraphx {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("StaticGraphx") 
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
        
        Edge(srcId, dstId, 1)
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系    
    //graph.persist(StorageLevel.MEMORY_AND_DISK_SER)  报错    java.lang.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level
    val graph = Graph(verticeRDD, edgeRDD)
     
    val ranks=graph.pageRank(0.001).vertices
    
    val DegInGraph = graph.outerJoinVertices(graph.inDegrees){
      (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0))}
    
    val DegInOutGraph = DegInGraph.outerJoinVertices(graph.outDegrees){
      (vid, p, outDegOpt) => (p._1, p._2, outDegOpt.getOrElse(0),  p._2+outDegOpt.getOrElse(0),
          math.abs(p._2-outDegOpt.getOrElse(0))/(p._2+outDegOpt.getOrElse(0)).toDouble )}
    
    val DegGraph = DegInOutGraph.outerJoinVertices(ranks){
      (vid, p, rankOpt) => (p._1, p._2, p._3, p._4, p._5,  rankOpt.getOrElse(0.0))
    }
    
    
    val SortedDegRatio = DegGraph.vertices.sortBy(_._2._6, false)
    SortedDegRatio.saveAsTextFile(args(1) + "SortedPagerank")   //按出度排序
    
    
     
    
  
     
    sc.stop()
  }  
}





