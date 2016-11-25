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
          math.abs(p._2._1-q.getOrElse(0.0,0,0,0)._1)/(p._2._1+q.getOrElse(0.0,0,0,0)._1).toDouble, p._2._3+q.getOrElse(0.0,0,0,0)._3   
      )
     }
    //card, 金额in，次数in，金额out， 次数out， 总金额，总次数， 金额净差, 净总比,总异地笔数
    
    //SumInOutGraph.vertices.collect().foreach(println)
    
    
     val SortedAmountIn = SumInOutGraph.vertices.sortBy(_._2._2, false)
     SortedAmountIn.saveAsTextFile(args(1) + "SortedAmountIn")   // 
    
     val SortedAmountOut= SumInOutGraph.vertices.sortBy(_._2._4, false)
     SortedAmountOut.saveAsTextFile(args(1) + "SortedAmountOut")   // 
     
     val SortedAmount = SumInOutGraph.vertices.sortBy(_._2._6, false)
     SortedAmount.saveAsTextFile(args(1) + "SortedAmount")   // 
    
     
     val SortedCountIn = SumInOutGraph.vertices.sortBy(_._2._3, false)
     SortedCountIn.saveAsTextFile(args(1) + "SortedCountIn")   // 
    
     val SortedCountOut= SumInOutGraph.vertices.sortBy(_._2._5, false)
     SortedCountOut.saveAsTextFile(args(1) + "SortedCountOut")   // 
     
     val SortedCount = SumInOutGraph.vertices.sortBy(_._2._7, false)
     SortedCount.saveAsTextFile(args(1) + "SortedCount")   // 
     
     val SortedExp = SumInOutGraph.vertices.sortBy(_._2._8, false)
     SortedExp.saveAsTextFile(args(1) + "SortedExp")   // 
	 
	   val SortedRatio = SumInOutGraph.vertices.sortBy(_._2._9, false)
     SortedRatio.saveAsTextFile(args(1) + "SortedRatio")   // 
     
     val SortedForeign = SumInOutGraph.vertices.sortBy(_._2._10, false)
     SortedForeign.saveAsTextFile(args(1) + "SortedForeign")   // 

    
//链接分析    
    val DegInGraph = graph.outerJoinVertices(graph.inDegrees){
      (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0))}
    
//相当于 SELECT v.id, v1.attr, v2.outDegree from vertices v1
//OUT JOIN (
//  select id, count(outDegrees) as outDegree from vertices
//) v2 ON v1.id = v2.id
    
    val DegInOutGraph = DegInGraph.outerJoinVertices(graph.outDegrees){
      (vid, p, outDegOpt) => (p._1, p._2, outDegOpt.getOrElse(0),  p._2+outDegOpt.getOrElse(0),
          math.abs(p._2-outDegOpt.getOrElse(0))/(p._2+outDegOpt.getOrElse(0)).toDouble )}
    

    val pgranks = graph.pageRank(0.001).vertices
    val DegGraph = DegInOutGraph.outerJoinVertices(pgranks){
      (vid, p, rankOpt) => (p._1, p._2, p._3, p._4, p._5,  rankOpt.getOrElse(0.0))
    }
    
    
// (id,(卡号,入度,出度,总度数，出入度比,pagerank))    
      
    val SortedDegIn = DegGraph.vertices.sortBy(_._2._2, false)
    SortedDegIn.saveAsTextFile(args(1) + "SortedDegIn")  

    val SortedDegOut = DegGraph.vertices.sortBy(_._2._3, false)
    SortedDegOut.saveAsTextFile(args(1) + "SortedDegOut")   //按出度排序
    
    val SortedDeg = DegGraph.vertices.sortBy(_._2._4, false)
    SortedDeg.saveAsTextFile(args(1) + "SortedDeg")   //按出度排序
    
    val SortedDegRatio = DegGraph.vertices.sortBy(_._2._5, false)
    SortedDegRatio.saveAsTextFile(args(1) + "SortedDegRatio")   //按出度排序
    
    val SortedPgRank = DegGraph.vertices.sortBy(_._2._6, false)
    SortedPgRank.saveAsTextFile(args(1) + "SortedPgRank")   //按出度排序
    
 
    val MaxInOut = sc.parallelize(SortedDegOut.take(100000)).intersection(sc.parallelize(SortedDegIn.take(100000)))
    MaxInOut.saveAsTextFile(args(1) + "MaxInOut")
    
    val MostDeg = sc.parallelize(SortedDeg.take(100000))
                   .intersection(sc.parallelize(SortedDegRatio.take(100000)))
                   .intersection(sc.parallelize(SortedPgRank.take(100000)))
                    
                   
                   
                   
    val MostSum =  sc.parallelize(SortedAmount.take(100000))                 
                   .intersection(sc.parallelize(SortedCount.take(100000)))
                   .intersection(sc.parallelize(SortedRatio.take(100000)))
                   
              
    
    val MostSuspect = MostSum.join(MostDeg) 
    
    MostSuspect.saveAsTextFile(args(1) + "MostSuspect")
    
   
    
    
    
    
    
    //DegInOutGraph.vertices.filter(_._2._2 >1000)
    val threList = List(0,1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,300,400,500,600,700,800,900,1000,2000,3000,4000,5000,6000,7000,8000,9000,10000)
    var InCountListStr = ""
    var OutCountListStr = ""
    var DegCountListStr = ""
    
    threList.foreach(thre=>
       InCountListStr = InCountListStr + DegInOutGraph.vertices.filter(_._2._2 >=thre).count().toString() + "\n"
       )
 
    threList.foreach(thre=>
       OutCountListStr = OutCountListStr + DegInOutGraph.vertices.filter(_._2._3 >=thre).count().toString() + "\n"
       )
       
    threList.foreach(thre=>
       DegCountListStr = DegCountListStr + DegInOutGraph.vertices.filter(_._2._4 >=thre).count().toString() + "\n"
       )
       
       
    
 
    val resultStr = "InCountListStr:\n" + InCountListStr + "OutCountListStr:\n" + OutCountListStr + "DegCountListStr:\n" + DegCountListStr 
    
    sc.parallelize(List(resultStr)).saveAsTextFile(args(1) + "DegCountInfo")
    
     
     
    sc.stop()
  }  
}






//spark-submit \
//--class WireFraud.StaticInOut \
//--master yarn \
//--deploy-mode cluster \
//--queue root.default \
//--driver-memory 7g \
//--executor-memory 7G \
//--num-executors 500 \
//TeleTrans.jar \
//TeleTrans/StaticMD51516 TeleTrans/InOut1516/










