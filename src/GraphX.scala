import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable 
import scala.io.Source  


object GraphxNet {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)
     
    // 读入时指定编码  
    val vFile = sc.textFile("xrli/CardDict/part*") 
    
    val verticeRDD = vFile.map { line=>
        val lineArray = line.split("\\s+")
        val vid = lineArray(0).toLong
        val card = lineArray(1)
        (vid,card)
    }
    

    val eFile = sc.textFile("xrli/AllTransNew/part*") 
    val edgeRDD = eFile.map { line=>
        val lineArray = line.split("\\s+")

        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        val amount = lineArray(2).toDouble
        val count = lineArray(3).toInt
          
       Edge(srcId, dstId, (amount,count))
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD) 
    
    //边操作：找出图中属性大于5的边
    println("找出图中交易次数大于4的边：")
    graph.edges.filter{e => 
      val amount = e.attr._1
      val count = e.attr._2
      count > 4
    }.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    println("vertices :")
    graph.vertices.collect().foreach(println)
    
      //个人理解 graph.inDegrees就是图中每个顶点对应的入读的元组的集合RDD。((点1,1的入度),(点2,2的入度)...)
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    
    inDegrees.collect().foreach(v => println("vid: " + v._1 + " inDeg: " + v._2))
    
    val DegGraph = graph
      .outerJoinVertices(graph.inDegrees){
      (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0))}
  
    println("连接图的属性：")
    DegGraph.vertices.collect.foreach(v => println("vid: " + v._1 + " card: " + v._2._1 + " inDeg: " + v._2._2))


    sc.stop()
  }
}