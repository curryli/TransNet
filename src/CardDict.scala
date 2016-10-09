import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable  
import collection.mutable.ArrayBuffer
import scala.io.Source  


object CardDict{
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)

    val textfile = sc.textFile("xrli/AllTrans/001098_0") 
 
    val rdd1 = textfile.map(line => line.split("\\001")).map(item=>(item(0)))
    val rdd2 = textfile.map(line => line.split("\\001")).map(item=>(item(1)))
    val AllCardList = rdd1.union(rdd2).distinct().collect()
    
    val CardArray = AllCardList.zipWithIndex
    
    val CardDictRDD = sc.parallelize(CardArray.map{
     case (card, idx) => idx.toString() + " " + card
    })
    CardDictRDD.saveAsTextFile("xrli/CardDict")
 
    val CardMap: mutable.HashMap[String,String] = new mutable.HashMap()
    for(pair <- CardArray){
      CardMap(pair._1) = pair._2.toString()
    }
    
    val result = textfile.map(line => line.split("\\001")).map(item=>CardMap(item(0))+ " " + CardMap(item(1))+ " " +item(2)+ " " +item(3)) 
    result.saveAsTextFile("xrli/AllTransNew")
    sc.stop()
  }
}