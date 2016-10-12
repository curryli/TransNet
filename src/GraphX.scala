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
    val vFile = sc.textFile("xrli/AntiLD/CardDict03/part*") 
    
    val verticeRDD = vFile.map { line=>
        val lineArray = line.split("\\s+")
        val vid = lineArray(0).toLong
        val card = lineArray(1)
        (vid,card)      //这里设置顶点编号就是前面的卡编号
    }
    

    val eFile = sc.textFile("xrli/AntiLD/TransEdgeFile03/part*") 
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
    

    println("vertices :")
    graph.vertices.saveAsTextFile("xrli/AntiLD/vertices03")
//graph.vertices.collect().foreach(println)
    
//vertices : (顶点编号，属性)
//(4904,a4618a3783e6b1c1db809e7a4bd6c89e)
//(1084,3c2253ad807c1daa10ffe6f33adb6a59)
    
    //边操作：找出图中属性大于5的边
    //println("找出图中交易次数大于4的边：")
    graph.edges.filter{e => 
      val amount = e.attr._1
      val count = e.attr._2
      count > 4
    }saveAsTextFile("xrli/AntiLD/frequentEdge03")         //.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    
//卡编号1 to 卡编号2  att (金额,次数)
//231 to 7602 att (210000.0,5)
//675 to 16285 att (6500000.0,5)



    

    val inDegrees: VertexRDD[Int] = graph.inDegrees
    //inDegrees.collect().foreach(v => println("vid: " + v._1 + " inDeg: " + v._2))
//个人理解 graph.inDegrees就是图中每个顶点对应的入读的元组的集合RDD。((点1,1的入度),(点2,2的入度)...)
//vid: 15839 inDeg: 1
//vid: 16599 inDeg: 1    
    
  
    val DegInGraph = graph.outerJoinVertices(graph.inDegrees){
      (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0))}
  
//    println("Graph attr: card and indeg:")
//    DegInGraph.vertices.collect.foreach(v => println("vid: " + v._1 + " card: " + v._2._1 + " inDeg: " + v._2._2))

//将顶点属性改为各点出度
//graph图本身可以看做一张表，主键是顶点编号，其他的属性是普通的列。    graph.inDegrees 也是一张表，主键也是顶点编号(使用outerJoinVertices必须要求主键相同)，相当于
//SELECT v.id, v1.attr, v2.outDegree from vertices v1
//OUT JOIN (
//select id, count(outDegrees) as outDegree from vertices
//) v2 ON v1.id = v2.id
//最后再对(v.id, v1.attr, v2.outDegree)进行变换操作 :  (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0)
//vid: 11202 card: e5e563913434decb62783521189e4699 inDeg: 1
//vid: 16438 card: 3c7f501a500165e2c0a78660806ed171 inDeg: 1
    
    val DegInOutGraph = DegInGraph.outerJoinVertices(graph.outDegrees){
      (vid, p, outDegOpt) => (p._1, p._2, outDegOpt.getOrElse(0))}
    
    val result =  DegInOutGraph.vertices.map(v => "vid: " + v._1 + " card: " + v._2._1 + " inDeg: " + v._2._2 + " OutDeg: " + v._2._3)
    result.saveAsTextFile("xrli/AntiLD/DegInout")
    
//    println("Graph attr: card, indeg,outdeg: ")
//    DegInOutGraph.vertices.collect.foreach(v => println("vid: " + v._1 + " card: " + v._2._1 + " inDeg: " + v._2._2 + " OutDeg: " + v._2._3))
// 
//同理，只是在前面的基础之上再增加一个    OutDeg
//vid: 15390 card: b3d3e2467749d63ebc6559507dac666c inDeg: 1 OutDeg: 0
//vid: 14082 card: 57084a23fe5ccf681696b049e266883a inDeg: 0 OutDeg: 1    
    
    
    sc.stop()
  }
}