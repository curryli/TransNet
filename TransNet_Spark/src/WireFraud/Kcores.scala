package WireFraud

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._


object KcoreTest {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("KTest") 
    val sc = new SparkContext(conf)
 
     val textfile = sc.textFile("hdfs://nameservice1/user/hive/warehouse/tele_sinout0708").persist(StorageLevel.MEMORY_AND_DISK_SER) 
    // 读入时指定编码  
    val rdd1 = textfile.map(line => line.split("\\001")(0).trim)                //.map(item => item(0))         
    val rdd2 = textfile.map(line => line.split("\\001")(1).trim)
    val AllCardList = rdd1.union(rdd2).distinct()
    
    val vFile = AllCardList
    
    val verticeRDD = vFile.map { line=>
        val vid = line.toLong
        val card = line
        (vid,card)      //这里设置顶点编号就是前面的卡编号
    }
    


    val edgeRDD = textfile.map { line=>
        val lineArray = line.split("\\001")

        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        val msum = lineArray(2).toDouble
        val csum = lineArray(3).toInt
        val fcsum = lineArray(4).toInt
          
       Edge(srcId, dstId, (msum, csum, fcsum))
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val transGraph = Graph(verticeRDD, edgeRDD) 
     

    var degreeGraph = transGraph.outerJoinVertices(transGraph.degrees) {
     (vid, oldData, newData) => newData.getOrElse(0)
    }.cache()                   //生成一个新图degreeGraph，每个图的顶点属性只有degree，即vertexRDD是(vid, degree)

    val kNum = 9
    var lastVerticeNum: Long = degreeGraph.numVertices     //计算图的顶点总数,返回Long型
    var thisVerticeNum: Long = -1
    var isConverged = false
    val maxIter = 1000
    var i = 1
    while (!isConverged && i <= maxIter) {  //删除 degre e< kNum 的顶点
      val subGraph = degreeGraph.subgraph(
      vpred = (vid, degree) => degree >= kNum
      ).cache()

      degreeGraph = subGraph.outerJoinVertices(subGraph.degrees) {   //重新生成新的degreeGraph
       (vid, vd, degree) => degree.getOrElse(0)
      }.cache()

      thisVerticeNum = degreeGraph.numVertices
    
      if (lastVerticeNum == thisVerticeNum) {
        isConverged = true
        println("vertice num is " + thisVerticeNum + ", iteration is " + i)
      } else {
        println("lastVerticeNum is " + lastVerticeNum + ", thisVerticeNum is " + thisVerticeNum + ", iteration is " + i + ", not converge")
        lastVerticeNum = thisVerticeNum
      }
      i += 1
  } 
  // do something to degreeGraph 
    
    //degreeGraph.edges.saveAsTextFile("xrli/degreeGraphEdges")
 
      
    degreeGraph.vertices.saveAsTextFile("xrli/TeleTrans/9KcoreV")
    
    sc.stop()
  }
}