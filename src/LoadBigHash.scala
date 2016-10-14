import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable 
import scala.io.Source  
import org.apache.spark.storage.StorageLevel


object LoadBigHash {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)
     
    
    val textfile = sc.textFile("xrli/HiveTrans03").persist(StorageLevel.MEMORY_AND_DISK_SER) 
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
    

    println("vertices :")
    graph.vertices.saveAsTextFile("xrli/Hashvertices")
    //graph.vertices.collect().foreach(println)
    
    println("edges :")
    graph.edges.saveAsTextFile("xrli/Hashedges")
    
    sc.stop()
  }
  
  
  
  def BKDRHash( str:String) :Long ={
   val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
   var hash:Long  = 0
   for(i <- 0 to str.length-1){
    hash = hash * seed + str.charAt(i)
    hash = hash & 0x7FFFFFFF
   }
   return hash 
}
  
}