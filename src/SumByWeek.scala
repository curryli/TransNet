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


object SumByWeek {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)
     
    //hadoop fs -cp hdfs://nameservice1/user/hive/warehouse/teletransweek TeleTrans/teletransweek
    val textfile = sc.textFile("TeleTrans/teletransweek").persist(StorageLevel.MEMORY_AND_DISK_SER) 
    // 读入时指定编码  
     
    val PeriodDicts: mutable.HashMap[Int, Int] = new mutable.HashMap[Int, Int]()

       for (i <- 0 to 356){
         //println(i)
         PeriodDicts(i) =  i/7
       }
                
                
    val Datefile = sc.textFile("TeleTrans/DateDicts.txt")
    val DataMap = Datefile.map{line=>
     val Token = line.split("\\s+") 
     (Token(0), PeriodDicts(Token(1).toInt))  
    }.collectAsMap()
  
    
    
    val outrdd = textfile.map{line=>
      val Token = line.split("\\001")
      val tfr_out_acct_no = Token(0)
      val dateperiod = DataMap(Token(1))
      val amount = Token(2).toDouble 
      val count = Token(3).toDouble
      ((tfr_out_acct_no, dateperiod), (amount, count))
    }
    
	  val SumByWeekRDD = outrdd.reduceByKey((x,y) => (x._1 + y._1 , x._2  + y._2 ))
	  
	  val SumByWeek = SumByWeekRDD.map(temp =>
      (temp._1._1, temp._1._2, temp._2._1, temp._2._2)   
    ) //.saveAsTextFile("TeleTrans/SumByWeek")
	  
	  
	  //SumByWeek.filter(temp => temp._3.matches("^.*[.].*[.].*$")).saveAsTextFile("TeleTrans/test")
    
    var Amountlist = SumByWeek.map(temp=>(temp._1,temp._3)).combineByKey(
      (v : Double) => List(v),
      (c : List[Double], v : Double) => v :: c,
      (c1 : List[Double], c2 : List[Double]) => c1 ::: c2
      )
    
   //  Amountlist.mapValues(x=>getVar(x)).saveAsTextFile("TeleTrans/AmountVar")
     Amountlist.mapValues(x=>getCV(x)).saveAsTextFile("TeleTrans/AmountCV")
     
    sc.stop()
  }
    
  
    def getCV(alist :List[Double]) :(Double, Double, List[Double]) ={
        var avg  = 0.0 
        val len = alist.length
        for (i <- 0 to (len-1)) {
          avg = avg + alist(i) ;
        }

        avg = avg / len;
        (getCV(alist, avg), avg,  alist)
     
    }
    
    
    def getCV(alist :List[Double], avg:Double) :Double = {
        val len = alist.length;
        var ff = 0.0;
        for (i <- 0 to (len-1)) {
            ff = ff + (alist(i) - avg) * (alist(i) - avg);
        }
        
        ff = ff / len;
        ff = ff/avg
        return ff;
    }
    
  
    
    
      
//  def getVar(alist :List[Double] ) :Double ={
//        var avg  = 0.0 
//        val len = alist.length
//        for (i <- 0 to (len-1)) {
//          avg = avg + alist(i);
//        }
//
//        avg = avg / len;
//        return getVar(alist, avg);
//    }
//    
//    def getVar(alist :List[Double], avg:Double) :Double = {
//        val len = alist.length;
//        var ff = 0.0;
//        for (i <- 0 to (len-1)) {
//            ff = ff + (alist(i)  - avg) * (alist(i) - avg);
//        }
//        
//        ff = ff / len;
//        return ff;
//    }
//    
}