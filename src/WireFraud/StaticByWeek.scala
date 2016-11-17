package WireFraud

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import scala.collection.mutable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.mutable.HashSet

object StaticByWeek {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("StaticByWeek") 
    val sc = new SparkContext(conf)
     
    //hadoop fs -cp hdfs://nameservice1/user/hive/warehouse/teletransweek TeleTrans/teletransweek
    val textfile = sc.textFile("TeleTrans/trans0405/*").persist(StorageLevel.MEMORY_AND_DISK_SER) 
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
      val tfr_in_acct_no = Token(0)
      val dateperiod = DataMap(Token(3))
      val trans_at = Token(2).toDouble 
      val acpt_id = if(Token(5).size>4) Token(5).substring(4) else "0000"
      val foreign_count = if(Token(6).equals("2")) 1 else  0
      val one = 1
      ((tfr_in_acct_no, dateperiod), (trans_at, one,  acpt_id, foreign_count))
    }
    
    

    val createCombiner = (v: (Double, Int, String, Int)) => {  
      (v._1,v._2, HashSet[String](v._3), v._4)
    }  
    
    val mergeValue = (c:(Double, Int, HashSet[String], Int), v:(Double, Int, String, Int)) => {  
      (c._1 + v._1, c._2 + v._2, c._3 + v._3, c._4 + v._4)  
    }  
    
    val mergeCombiners = (c1:(Double, Int, HashSet[String], Int), c2:(Double, Int, HashSet[String], Int))=>{  
      (c1._1 + c2._1, c1._2 + c2._2, c1._3 union c2._3, c1._4 + c2._4)  
    }  
    
     
	  val SumByWeek= outrdd.combineByKey(
	    createCombiner, 
      mergeValue,  
      mergeCombiners 
	  ) .map(temp => (temp._1._1, temp._1._2, temp._2._1, temp._2._2, temp._2._3.size,temp._2._4))  //.sortBy(_._5, false)  
	     //key（卡号，时间段），  value（交易金额, 交易次数, 交易地点数, 异地交易次数）
	
	   //SumByWeek.take(5).foreach(println)
	  
 
	   SumByWeek.saveAsTextFile("TeleTrans/WeekResult")
	  
	  
//	  //SumByWeek.filter(temp => temp._3.matches("^.*[.].*[.].*$")).saveAsTextFile("TeleTrans/test")
//    
//    var Amountlist = SumByWeek.map(temp=>(temp._1,temp._3)).combineByKey(
//      (v : Double) => List(v),
//      (c : List[Double], v : Double) => v :: c,
//      (c1 : List[Double], c2 : List[Double]) => c1 ::: c2
//      )
//    
//   //  Amountlist.mapValues(x=>getVar(x)).saveAsTextFile("TeleTrans/AmountVar")
//     Amountlist.mapValues(x=>getCV(x)).saveAsTextFile("TeleTrans/AmountCV")
     
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












//val outrdd = textfile.map{line=>
//      
//      val Token = line.split("\\001")
//      val tfr_in_acct_no = Token(0)
//      val dateperiod = DataMap(Token(3))
//      val trans_at = Token(2).toDouble 
//      
//      val acpt_id = if(Token(5).size>4) Token(5).substring(4) else "0000"
//	    val locSet: HashSet[String] =  HashSet[String](acpt_id)
// 
//      val foreign_count = if(Token(6).equals("2")) 1 else  0
//      val one = 1
//      ((tfr_in_acct_no, dateperiod), (trans_at, one,  locSet, foreign_count))
//    }
//    
//    
//
//    
//      def staticWeek(x:(Double, Int, HashSet[String], Int), y:(Double, Int, HashSet[String], Int)) :(Double, Int, HashSet[String], Int) = {
//        
//        val trans_sum = x._1 + y._1
//	      val trans_count = x._2  + y._2
//	     
//	      val locSet_union = x._3  union y._3
//	      val foreign_sum = x._4  + y._4
//	      //(trans_sum , trans_count, "aaa", foreign_sum)
//         (trans_sum , trans_count, locSet_union, foreign_sum)
//    }
//	  
//    
//	  val SumByWeek= outrdd.reduceByKey(
//	      staticWeek
//	  ) .map(temp => (temp._1._1, temp._1._2, temp._2._1, temp._2._2, temp._2._3,temp._2._4)) //.saveAsTextFile("TeleTrans/SumByWeek")
//	     