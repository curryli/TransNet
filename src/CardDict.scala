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

    val textfile = sc.textFile("xrli/AllTrans/0*") 
 
    val rdd1 = textfile.map(line => line.split("\\001")).map(item=>(item(0)))
    val rdd2 = textfile.map(line => line.split("\\001")).map(item=>(item(1)))
    val AllCardList = rdd1.union(rdd2).distinct().collect()
    
    val CardArray = AllCardList.zipWithIndex
    
    val CardDictRDD = sc.parallelize(CardArray.map{
     case (card, idx) => idx.toString() + " " + card
    })
    CardDictRDD.saveAsTextFile("xrli/CardDict03")
 
    val CardMap: mutable.HashMap[String,String] = new mutable.HashMap()
    for(pair <- CardArray){
      CardMap(pair._1) = pair._2.toString()
    }
    
    val result = textfile.map(line => line.split("\\001")).map(item=>CardMap(item(0))+ " " + CardMap(item(1))+ " " +item(2)+ " " +item(3)) 
    result.saveAsTextFile("xrli/AllTrans03")
    sc.stop()
  }
}



//"xrli/AllTrans/0*"
//加密卡1 加密卡2 交易金额   交易次数
//00065df18cbe6464034dce110c84c7c1b939353944d36442a9891f673dcb3a1a20000.01
//001b6183fe3950476f32c48bea47d6c6eeb7aebb38d5599b84f5448291f757cf690000.01
//00281b32e4b00d532a248f867ac343976a02f34483c6b683d77d691a7f36624250000.01
//002ec480d828c3cedfd2108cd079897d20f0c1709b0a781c07cbc6ba26391ffe40000.02
//00338c54e1ef07c452ce4e7bc77f081c2499d2fa03e877e5c0c1e213c5dc6b40100000.01

//目的：将所有出现的卡号进行（去重后）编号

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