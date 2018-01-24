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
TeleTrans.jar "20170511" "20170511" "xrli/AML/Inputs/seedCards_test.csv" "xrli/AML/Outputs/CardsScore_test.csv"
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

object ScoreCardsQueue {

 

  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);

    val sparkConf = new SparkConf().setAppName("HelloSpark")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    println("Hello spark")

    val rdd01 = sc.makeRDD(List(1,2,3,4,5,6))
    //rdd01.saveAsTextFile("/user/hdrisk/output_card/testout")
    rdd01.saveAsTextFile("/user/hdanaly/xrli/AML/testout")
    
    
    sc.stop()

  }
 }

  
  
   



// select fwd_settle_conv_rt, trans_curr_cd from tbl_common_his_trans where pdate='20170511' and  fwd_settle_conv_rt="30001000" limit 2;   61000000: 156