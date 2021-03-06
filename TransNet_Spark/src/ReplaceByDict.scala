import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable  
import collection.mutable.ArrayBuffer
import scala.io.Source  


object ReplaceByDict{
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)

    val textfile = sc.textFile("xrli/AntiLD/CardDict03/*")

    val CardRDD = textfile.map{line=>
      val id = line.split("\\s+")(0)
      val card = line.split("\\s+")(1)
      (card, id)
    }
     
    val CardMap = CardRDD.collectAsMap()
    
    
    val filetochange = sc.textFile("xrli/AntiLD/HiveTrans03/*")
    val result = filetochange.map(line => line.split("\\001")).map(item=>CardMap(item(0))+ " " + CardMap(item(1))+ " " +item(2)+ " " +item(3)) 
    result.saveAsTextFile("xrli/AntiLD/TransEdgeFile03")
 

    sc.stop()
  }
  
  
  
  
}



//"xrli/CardDict03"
//编号  卡号
//0 d5a13daaad772bc1ed3cf0dfc8b2a167
//1 dc6242f627355b37fe1a71a5dc84ddf5
//2 08726de10cc6b4ef7855817310f41e36
//3 c94d54f5e90e4f172c659e56e2b87b08
//4 e416cc0fb184d6db4ce4673512d2243d

//"xrli/AllTrans03"
//卡编号1 卡编号2 交易金额  交易次数
//3878 9409 20000.0 1
//1816 14809 690000.0 1
//5930 13215 50000.0 1
//13291 10069 40000.0 2
//17263 3776 100000.0 1