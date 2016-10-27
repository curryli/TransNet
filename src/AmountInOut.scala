import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable 
import scala.io.Source  
import org.apache.spark.storage.StorageLevel


object AmountInOut {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SortDegree") 
    val sc = new SparkContext(conf)
     
    
    val textfile = sc.textFile("xrli/HiveTrans03").persist(StorageLevel.MEMORY_AND_DISK_SER)       //xrli/testfile.txt   xrli/HiveTrans03
    
    
    // 读入时指定编码  
    val rdd1 = textfile.map(line => line.split("\\001")(0).trim)                //.map(item => item(0))         
    val rdd2 = textfile.map(line => line.split("\\001")(1).trim)
    val AllCardList = rdd1.union(rdd2).distinct()
    
    val vFile = AllCardList
    
    val verticeRDD = vFile.map { line=>
        val vid = BKDRHash(line)
        val card = line
        (vid,card)      //这里设置顶点编号就是前面的卡编号
    }
 
    val edgeRDD = textfile.map { line=>
        val lineArray = line.split("\\001")

        val srcId = BKDRHash(lineArray(0))
        val dstId = BKDRHash(lineArray(1))
        val amount = lineArray(2).toDouble
        val count = lineArray(3).toInt
          
       Edge(srcId, dstId, (amount,count))
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD) 
     
    val SumInVRDD = graph.aggregateMessages[(Double,Int)](
      triplet => {   
          triplet.sendToDst(triplet.attr);         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数)，所以不管是转出金额还是转入金额我们都累加起来
     },
      (a, b) =>  (a._1 + b._1,  a._2 + b._2)
    )
    
    val SumOutVRDD = graph.aggregateMessages[(Double,Int)](
      triplet => {   
          triplet.sendToSrc(triplet.attr)         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数)，所以不管是转出金额还是转入金额我们都累加起来
     },
      (a, b) =>  (a._1 + b._1,  a._2 + b._2)
    )
    
    
     val SumInGraph = graph.outerJoinVertices(SumInVRDD){
      (vid, card, q) => (card, q.getOrElse((0.0,0)))
     }
    
    val SumInOutGraph = SumInGraph.outerJoinVertices(SumOutVRDD){
      (vid, p, q) => (p._1, p._2._1,p._2._2, q.getOrElse(0.0,0)._1, q.getOrElse(0.0,0)._2, p._2._1+q.getOrElse(0.0,0)._1, p._2._2+q.getOrElse(0.0,0)._2 )
     }
    //card, 金额in，次数in，金额out， 次数out， 总金额，总次数
    
    //SumInOutGraph.vertices.collect().foreach(println)
    
    
     val SortedAmountIn = SumInOutGraph.vertices.sortBy(_._2._2, false)
     SortedAmountIn.saveAsTextFile("xrli/TransNettest/SortedAmountIn")   //按出度排序
    
     val SortedAmountOut= SumInOutGraph.vertices.sortBy(_._2._4, false)
     SortedAmountOut.saveAsTextFile("xrli/TransNettest/SortedAmountOut")   //按出度排序
     
     val SortedAmount = SumInOutGraph.vertices.sortBy(_._2._6, false)
     SortedAmount.saveAsTextFile("xrli/TransNettest/SortedAmount")   //按出度排序
    
     
     val SortedCountIn = SumInOutGraph.vertices.sortBy(_._2._3, false)
     SortedCountIn.saveAsTextFile("xrli/TransNettest/SortedCountIn")   //按出度排序
    
     val SortedCountOut= SumInOutGraph.vertices.sortBy(_._2._5, false)
     SortedCountOut.saveAsTextFile("xrli/TransNettest/SortedCountOut")   //按出度排序
     
     val SortedCount = SumInOutGraph.vertices.sortBy(_._2._7, false)
     SortedCount.saveAsTextFile("xrli/TransNettest/SortedCount")   //按出度排序
     
    
    
//    val SumVRDD = graph.aggregateMessages[(Double,Int)](
//
//      triplet => {   
//          triplet.sendToDst(triplet.attr);         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数)，所以不管是转出金额还是转入金额我们都累加起来
//          triplet.sendToSrc(triplet.attr)
//     },
//
//      (a, b) =>  (a._1 + b._1,  a._2 + b._2)
//    )
//    
//    val ResultGraph = graph.outerJoinVertices(SumVRDD){
//      (vid, card, q) => (card, q.getOrElse((0.0,0)))
//   }
//    //p:  card    q: case(amountsum,countsum)  
//    
//    val SortedAmount = ResultGraph.vertices.sortBy(_._2._1, false)
//    SortedAmount.saveAsTextFile("xrli/TransNet/SortedAmount")   //按出度排序
//    
//    val SortedCount = ResultGraph.vertices.sortBy(_._2._2, false)
//    SortedCount.saveAsTextFile("xrli/TransNet/SortedCount")   //按出度排序
    
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