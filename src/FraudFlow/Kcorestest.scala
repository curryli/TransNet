package FraudFlow 
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import SparkContext._ 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._  
import org.apache.spark.sql.types._ 
import AlgorithmUtil.KcoresLabel


object Kcorestest {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);
 
    //设置运行环境
    val sparkConf = new SparkConf().setAppName("sparkSQL")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)  
 
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, "a"),
      (2L, "b"),
      (3L, "c"),
      (4L, "d"),
      (5L, "e"),
      (6L, "f"),
      (7L, "g"),
      (8L, "h")
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(1L, 2L, 99),
      Edge(2L, 3L, 99),
      Edge(3L, 1L, 99),
      Edge(2L, 4L, 99),
      Edge(4L, 5L, 99),
      Edge(5L, 2L, 99),
      Edge(5L, 6L, 99),
      Edge(3L, 6L, 99),
      Edge(7L, 8L, 99)
    )
 
    //构造vertexRDD和edgeRDD
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
 
    //构造图Graph[VD,ED]
    var graph = Graph(vertexRDD, edgeRDD)
    val KcoreVertices =  KcoresLabel.maxKVertices(graph,2)
    println("maxKVertices labeled vertices: ")
    KcoreVertices.collect().foreach(println)
    
    
    val result = AlgorithmUtil.KcoresLabel.KLabeledVertices(graph, 9, sc)
    
    println("KLabeledVertices  vertices: ")
    result.collect().foreach(println)
     
    sc.stop()
}
  
} 