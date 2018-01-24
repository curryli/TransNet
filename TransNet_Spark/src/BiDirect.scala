
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
 
object BiDirect {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)
 
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 3L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 4L, 3)
    )
 
    //构造vertexRDD和edgeRDD
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
 
    //构造图Graph[VD,ED]
    val graph = Graph(vertexRDD, edgeRDD)
 
    println("indeg")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.collect().foreach(println)
    
    println("Outdeg")
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.collect().foreach(println)
    
    println("Alldeg")
    val Degrees: VertexRDD[Int] = graph.degrees
    Degrees.collect().foreach(println)
    
    
    sc.stop()
  }
}


//结果：
//indeg
//(1,1)
//(2,1)
//(3,1)
//(4,1)
//Outdeg
//(2,2)
//(3,2)
//Alldeg
//(1,1)
//(2,3)
//(3,3)
//(4,1)