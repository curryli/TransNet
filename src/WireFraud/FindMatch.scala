package WireFraud

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._
 

object FindMatch {
  def main(args: Array[String]) {
  //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("FindMatch") 
    val sc = new SparkContext(conf)
 
    val textfile1 = sc.textFile("xrli/TeleTrans/9KcoreV")
    
    
     
    val CardFromAll = textfile1.map { line=>
       line.split(",")(0).substring(1)
    }
    
    val CardFromBlack = sc.textFile("xrli/TeleTrans/suscard4hash")
    
    val MatchedCards = CardFromAll.intersection(CardFromBlack)
    MatchedCards.saveAsTextFile("xrli/TeleTrans/MatchedCards9")
    
    sc.stop()
  }
  
}