package Service
/*
spark-submit \
--class Service.ScoreReadCards \
--master yarn \
--deploy-mode cluster \
--queue root.default \
--driver-memory 15g \
--executor-memory 15G \
--num-executors 300 \
TeleTrans.jar "20170511" "20170511"
*/

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

import SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import AlgorithmUtil._
import scala.reflect.ClassTag
import scala.collection.mutable
import scala.collection.mutable.HashSet
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions 

object quicktest_ScoreMd5Cards {

  private val KMax = 5
 

  def any_to_double[T: ClassTag](b: T): Double = {
    if (b == true)
      1.0
    else
      0
  }

  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);

    val sparkConf = new SparkConf().setAppName("ScoreReadCards")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    
    sparkConf.set("spark.yarn.queue", "root.queue_hdrisk")
    sparkConf.set("queue", "root.queue_hdrisk")

    val startTime = System.currentTimeMillis();

//    hc.sql("add jar /opt/cloudera/parcels/CDH/lib/hive/auxlib/csv-serde-1.1.2.jar")
//    
//    println("Add csv-serde to hive")
    
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
      s"set mapred.max.split.size=10240000000" +
      s"set mapred.min.split.size.per.node=10240000000" +
      s"set mapred.min.split.size.per.rack=10240000000" +
      s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
      s"set mapreduce.job.queuename=root.queue_hdrisk")
    

 
    val startDate = args(0)
    val endDate = args(1)  

    
    val inputFile = "/user/hdrisk/AML/MD5_card"
    val outputFile = "/user/hdrisk/AML/output_Score"
    val MD5Pair = sc.textFile(inputFile).map{x => List(x.split(",")(0),x.split(",")(1))}   
    
    val Row_RDD = MD5Pair.map{f=>
           Row.fromSeq(f.toSeq)
       }
  
    val schemaPair = StructType(StructField("oricard",StringType,true)::StructField("md5card",StringType,true)::Nil)
    var MD5PairDF = sqlContext.createDataFrame(Row_RDD, schemaPair).persist(StorageLevel.MEMORY_AND_DISK_SER)
        
    val seedList = MD5Pair.map(f=>f(1)).collect() 
    
    
    
    var All_pairs = hc.sql(
        s"select tfr_in_acct_no," +
        s"tfr_out_acct_no " +
        s"from 00010000_default.viw_common_his_trans where " +
        s"pdate>=$startDate and pdate<=$endDate ")
      .toDF("srccard", "dstcard").repartition(5000) 
     
       
      val last_DF = MD5PairDF
     
      last_DF.rdd.map(_.mkString(",")).coalesce(1).saveAsTextFile(outputFile)
    
    
    
    
    println("All flow done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    sc.stop()

  }

  
  
  
  
  
  def antiSearch(All_pairs: DataFrame, beginDate: String, endDate: String, seedList: Array[String]) = {
      val maxitertimes =5
      var currentDataSize = seedList.length.toLong
 
      var lastDataSize=0L
  
      var tempData=null 
      var cardList=seedList
 
      var i=0
      
      while(i<maxitertimes && lastDataSize!=currentDataSize){
         i=i+1
         println("Start iteration " + i)
         var tempData= All_pairs.filter(All_pairs("srccard").isin(cardList: _*) or All_pairs("dstcard").isin(cardList: _*)).select(s"srccard",s"dstcard").distinct()    
         var dataFrame1=tempData.select(s"srccard").distinct()
         var dataFrame2=tempData.select(s"dstcard").distinct() 
         var temp= dataFrame1.unionAll(dataFrame2).distinct().map{r => r.getString(0)}
         
         currentDataSize=temp.count()
         println("currentDataSize: ", currentDataSize)
         cardList=temp.collect()  

         lastDataSize=currentDataSize
        }
      println("cardList", cardList.mkString(","))
      cardList
    }
  
  
  
  def dfZipWithIndex(df: DataFrame, offset: Int = 1, colName: String = "row_idx", inFront: Boolean = true) : DataFrame = {
      df.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map(ln =>
          Row.fromSeq(
            (if (inFront) Seq(ln._2 + offset) else Seq())
              ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
          )
        ),
        StructType(
          (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]()) 
            ++ df.schema.fields ++ 
          (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
        )
      ) 
  }
      
} 







// select fwd_settle_conv_rt, trans_curr_cd from tbl_common_his_trans where pdate='20170511' and  fwd_settle_conv_rt="30001000" limit 2;   61000000: 156