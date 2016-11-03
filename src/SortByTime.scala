
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable 
import scala.io.Source  
import org.apache.spark.storage.StorageLevel
import java.util.Locale  
import java.text.SimpleDateFormat  
import java.util.Date 

class MultiSort(val first:Long, val second:Int, val third:Int) extends Ordered[MultiSort] with Serializable{
  override def compare(that: MultiSort): Int = {
  if( this.first - that.first != 0 )
  {
     (this.first - that.first).toInt 
   }
   else if( this.second - that.second != 0 ){
     this.second - that.second 
   }
    else{
       this.third - that.third 
  }
 }
}

object SortByTime {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    conf.set("spark.kryoserializer.buffer.max","2000m");
    val sc = new SparkContext(conf)
     
    //hadoop fs -cp hdfs://nameservice1/user/hive/warehouse/teletransweek TeleTrans/teletransweek
    val textfile = sc.textFile("TeleTrans/TransSort/*")         //.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    //  "TeleTrans/TransTime/000132_0"
     
    val pairWithSortkey = textfile.map{line=>
      val Token = line.split("\\001")
      val tfr_out_acct_no = BKDRHash(Token(0))
      val tfr_in_acct_no = BKDRHash(Token(1))
      val trans_at = Token(2).toDouble 
      val pdate = Token(3).toInt 
      val loc_trans_tm = Token(4).toInt 
      val res = (tfr_out_acct_no, pdate, loc_trans_tm, tfr_in_acct_no, trans_at)
      (new MultiSort( tfr_out_acct_no, pdate, loc_trans_tm), res)  
    }
    
  
   val sorted = pairWithSortkey.sortByKey(true)

   //sorted.map(sortedline => sortedline._2).saveAsTextFile("TeleTrans/SortByTime")
    
   sorted.map(sortedline => sortedline._2).collect.foreach(println)  
     
    sc.stop()
  }
    
  
  def BKDRHash( str:String) :Long ={
   val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
   var hash:Long  = 0
   for(i <- 0 to str.length-1){
    hash = hash * seed + str.charAt(i)
    hash = hash.&("536870911".toLong)        //0x1FFFFFFF             //固定一下长度
   }
   return hash 
}
    
    
}



// val pairWithSortkey = textfile.map{line=>
//      ...
//       (tfr_out_acct_no, (pdate, loc_trans_tm, tfr_in_acct_no, trans_at))
//    }.groupByKey()
//
////结果 Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)), (3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))
//
//
//pairWithSortkey.map{ ArrayBuffer =>
//  val temp = ArrayBuffer.map{(......
//            new MultiSort(pdate, loc_trans_tm), res)  }	
//  temp.sortByKey(true)		
//}

