import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable  
import collection.mutable.ArrayBuffer
import scala.io.Source  


object JudgeHash{
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("JudgeHash") 
    val sc = new SparkContext(conf)

    val textfile = sc.textFile("xrli/Hashvertices/*")
    println("textfile length is : " + textfile.count())
    
    val hashRDD = textfile.map{line=>
      val linearray = line.split(",")
      linearray(0).substring(1)
    }
    
 
    println("hashRDD length is : " + hashRDD.distinct().count())
 
    
    sc.stop()
  }
  
  
}