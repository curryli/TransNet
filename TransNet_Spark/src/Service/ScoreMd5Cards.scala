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

object ScoreMd5Cards {

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
//    val inputFile = args(2)
//    val outputFile = args(3)
    
      
    //val seedList = sc.textFile("xrli/AML/Inputs/seedCards_test.csv").collect() 
    
    
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
      .toDF("srccard", "dstcard").repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)
      
     
    var transferList = antiSearch(All_pairs,startDate,endDate,seedList) 
    println("antiSearch done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
     
    All_pairs.unpersist(blocking = false)
    
    
    println("AntiSearch done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
    
    var Alldata = hc.sql(
      s"select tfr_in_acct_no," +
        s"tfr_out_acct_no, " +
        s"cast(trans_at as double), " +
        s"pdate, " +
        s"loc_trans_tm, " +
        s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
        s"trim(trans_md), " +
        s"mchnt_tp, " +
        s"mchnt_cd, " +
        s"trans_chnl, " +
        s"term_id, " +
        s"fwd_ins_id_cd, " +
        s"rcv_ins_id_cd, " +
        s"card_class, " +
        s"resp_cd4, " +
        s"acpt_ins_tp, " +
        s"auth_id_resp_cd, " +
        s"substring(acpt_ins_id_cd,0,4) as acpt_bank, " +
        s"cast(total_disc_at as double), " +
        s"trans_curr_cd, " +
        s"trans_id, " +
        s"pri_acct_no_conv " +
        //s"from tbl_common_his_trans where " +
        s"from 00010000_default.viw_common_his_trans where " +
        s"pdate>=$startDate and pdate<=$endDate ")
      .toDF("srccard", "dstcard", "money", "date", "time", "region_cd", "trans_md", "mchnt_tp", "mchnt_cd", "trans_chnl",
        "term_id", "fwd_ins_id_cd", "rcv_ins_id_cd", "card_class", "resp_cd4", "acpt_ins_tp", "auth_id_resp_cd", "acpt_bank", "charge", "trans_curr_cd", "trans_id", "card")
      .repartition(10000).persist(StorageLevel.MEMORY_AND_DISK_SER)
      
       
    var Useddata = Alldata.filter(Alldata("card").isin(seedList: _*)).repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var graphata =  Alldata.filter(Alldata("srccard").isin(transferList : _*) or Alldata("dstcard").isin(transferList : _*)).repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)
  
     
    
    
    //var graphata =  Alldata.filter(Alldata("srccard").isin(seedList : _*) or Alldata("dstcard").isin(seedList : _*))
     
    //graphata.show(5)
    
    Alldata.unpersist(blocking = false)
    
    
    println("All data obtained done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
     
    /////////////////////////////////////交易标记//////////////////////////////////////////         
    val get_hour = udf[Int, String] { xstr => 
       var result = 0
     try{
        result = xstr.substring(0, 2).toInt 
        }
     catch{
       case ex: java.lang.StringIndexOutOfBoundsException => {result = -1}
        } 
      result
    }
    
    Useddata = Useddata.withColumn("hour", get_hour(Useddata("time")))

    println("is_Night")
    val is_Night = udf[Double, String] { xstr =>
      val h = xstr.toInt
      val night_list = List(23, 0, 1, 2, 3, 4, 5)
      any_to_double(night_list.contains(h))
    }

    Useddata = Useddata.withColumn("is_Night", is_Night(Useddata("hour")))

    val getProvince = udf[String, String] { xstr => xstr.substring(0, 2) }
    Useddata = Useddata.withColumn("Prov", getProvince(Useddata("region_cd")))

    val getRMB = udf[Long, String] { xstr => (xstr.toDouble / 100).toLong }
    Useddata = Useddata.withColumn("RMB", getRMB(Useddata("money")))

    val is_RMB_500 = udf[Double, Long] { xstr => any_to_double(xstr.toDouble % 500 == 0) }
    Useddata = Useddata.withColumn("is_bigRMB_500", is_RMB_500(Useddata("RMB")))

    val is_RMB_1000 = udf[Double, Long] { xstr => any_to_double(xstr.toDouble % 1000 == 0) }
    Useddata = Useddata.withColumn("is_bigRMB_1000", is_RMB_1000(Useddata("RMB")))

    //println("智策大额整额定义")
    println("is_large_integer")
    val is_large_integer = udf[Double, Long] { a =>
      val b = a.toString.size
      val c = a.toDouble / (math.pow(10, (b - 1)))
      val d = math.abs(c - math.round(c))
      val e = d.toDouble / b.toDouble
      any_to_double(e < 0.01 && a > 1000)
    }
    Useddata = Useddata.withColumn("is_large_integer", is_large_integer(Useddata("RMB")))

    //println("交易金额中8和9的个数")
    println("count_89")
    val count_89 = udf[Double, String] { xstr =>
      var cnt = 0
      xstr.foreach { x => if (x == '8' || x == '9') cnt = cnt + 1 }
      cnt.toDouble
    }
    Useddata = Useddata.withColumn("count_89", count_89(Useddata("money")))

    println("is_highrisk_loc")
    val HighRisk_Loc = List("1425", "4050", "4338", "5624", "5923", "6123", "6431", "3974", //电信诈骗
      "5810", "5972", "5880", "6054", "6582", "6852", "6851", "6762", "7035", "7095", "6314", "6366", "6266", "6927", "8991", "7517", "7580", "7517", "3371", "3336", "3724", "7975", "7910", "5119", "5118", "8737", "8360", "6125", "5625", "7035", "6754", "6900", "7155", "7096", "6574", "6761", "6717", "7091", "5546", "6623", "7039", "6755", "5654", "6900", "6900", "5210", "7348") //涉毒
    val is_highrisk_loc = udf[Double, String] { xstr => any_to_double(HighRisk_Loc.contains(xstr.substring(0, 2))) }
    Useddata = Useddata.withColumn("is_highrisk_loc", is_highrisk_loc(Useddata("region_cd")))

    //println("持卡人原因导致的失败交易")
    println("is_cardholder_fail")
    val is_cardholder_fail = udf[Double, String] { xstr => any_to_double(List("51", "55", "61", "65", "75").contains(xstr)) }
    Useddata = Useddata.withColumn("is_cardholder_fail", is_cardholder_fail(Useddata("resp_cd4")))

    //println("是否正常汇率(这里用人民币币种代替)")
    println("not_norm_rate")
    val not_norm_rate = udf[Double, String] { xstr => any_to_double(xstr != "156") }
    Useddata = Useddata.withColumn("not_norm_rate", not_norm_rate(Useddata("trans_curr_cd")))

    val isForeign = udf[Double, String] { xstr => any_to_double(!xstr.equals("1")) }
    Useddata = Useddata.withColumn("isForeign", isForeign(Useddata("trans_md")))

    /////////////////////////////////////////////////////////////////////////////////////////////////////     

    ////////////////////////整体group///////////////////////////////    
    val tot_agg = Useddata.groupBy("card")
    var stat_DF = tot_agg.agg(sum("is_Night") as "Night_cnt").repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("1")
    
    var tmp_DF = tot_agg.agg(countDistinct("region_cd") as "tot_regions")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_regions"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(countDistinct("term_id") as "tot_term_ids")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_term_ids"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(countDistinct("Prov") as "tot_Provs")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_Provs"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("is_bigRMB_500") as "tot_big500")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_big500"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("is_bigRMB_1000") as "tot_big1000")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_big1000"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("is_large_integer") as "tot_large_integer")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_large_integer"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("is_highrisk_loc") as "tot_HRloc_trans")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_HRloc_trans"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("is_cardholder_fail") as "tot_cardholder_fails")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_cardholder_fails"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("not_norm_rate") as "tot_abnorm_rate")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_abnorm_rate"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("count_89") as "tot_count_89")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_count_89"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = tot_agg.agg(sum("isForeign") as "tot_Foreign")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_Foreign"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(count("region_cd") as "tot_Counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_Counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("RMB") as "tot_Amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_Amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")
      
    /////////////////////////////////////////////////////////////////////////////////////////////////////         

    var transinData = Useddata.filter(Useddata("trans_id").===("S33"))
    var transoutData = Useddata.filter(Useddata("trans_id").===("S25"))
    var quxianData = Useddata.filter(Useddata("trans_id").===("S24"))
    var queryData = Useddata.filter(Useddata("trans_id").===("S00"))
    var consumeData = Useddata.filter(Useddata("trans_id").===("S22"))
    var daifuData = Useddata.filter(Useddata("trans_id").===("S31"))

    val transin_gb = transinData.groupBy("card")
    val transout_gb = transoutData.groupBy("card")
    val quxian_gb = quxianData.groupBy("card")
    val query_gb = queryData.groupBy("card")
    val consume_gb = consumeData.groupBy("card")
    val daifu_gb = daifuData.groupBy("card")
     

    println("3")
    ////////////////////////整体group///////////////////////////////   

    tmp_DF = transin_gb.agg(count("RMB") as "transin_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transin_gb.agg(sum("RMB") as "transin_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transin_gb.agg(avg("RMB") as "transin_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transin_gb.agg(avg("RMB") as "transin_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_max"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transin_gb.agg(countDistinct("srccard") as "distinct_cards_in")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("distinct_cards_in"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")
 
    tmp_DF = transout_gb.agg(count("RMB") as "transout_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transout_gb.agg(sum("RMB") as "transout_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transout_gb.agg(avg("RMB") as "transout_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transout_gb.agg(avg("RMB") as "transout_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_max"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = transin_gb.agg(countDistinct("dstcard") as "distinct_cards_out")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("distinct_cards_out"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = quxian_gb.agg(count("RMB") as "quxian_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = quxian_gb.agg(sum("RMB") as "quxian_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = quxian_gb.agg(avg("RMB") as "quxian_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = quxian_gb.agg(avg("RMB") as "quxian_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_max"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = consume_gb.agg(count("RMB") as "consume_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = consume_gb.agg(sum("RMB") as "consume_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = consume_gb.agg(avg("RMB") as "consume_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = consume_gb.agg(avg("RMB") as "consume_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_max"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")
    
    
    tmp_DF = daifu_gb.agg(count("RMB") as "daifu_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("daifu_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = daifu_gb.agg(sum("RMB") as "daifu_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("daifu_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = daifu_gb.agg(avg("RMB") as "daifu_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("daifu_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")

    tmp_DF = daifu_gb.agg(avg("RMB") as "daifu_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("daifu_max"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card") === tmp_DF("card_2"), "left_outer").drop("card_2")
    
 
    //stat_DF.show() 
    stat_DF.repartition(5000).persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    Useddata.unpersist(blocking=false)
    
    
    println("Stage1 done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
    
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    println("start stating graph props.")
    
    import sqlContext.implicits._
    val InPairRdd = graphata.map(line => (HashEncode.HashMD5(line.getString(0)), line.getString(0)))
    val OutPairRdd = graphata.map(line => (HashEncode.HashMD5(line.getString(1)), line.getString(1)))
    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()

    //println(verticeRDD.count())

    val edgeRDD = graphata.map { line =>
      val srcId = HashEncode.HashMD5(line.getString(0))
      val dstId = HashEncode.HashMD5(line.getString(1))
      val money = line.getDouble(2) / 100

      val pdate = line.getString(3)
      val trans_hour = line.getString(4).substring(0, 2).toInt
      val isnight = if (trans_hour > 0 && trans_hour < 6) 1 else 0 //是否夜间交易  1是 0否
      val time_detail = pdate + line.getString(4)

      val region_cd = line.getString(5)
      val isForeign = if (!line.getString(6).equals("1")) 1 else 0 // 异地交易次数+1            交易模式    1 同城   2 异地    4 跨境
      val mchnt_tp = line.getString(7)
      val mchnt_cd = line.getString(8)

      val trans_chnl = line.getString(9)
      val term_id = line.getString(10)
      val fwd_ins_id_cd = line.getString(11)
      val rcv_ins_id_cd = line.getString(12)
      val card_class = line.getString(13)
      val resp_cd4 = line.getString(14)
      val acpt_ins_tp = line.getString(15)
      val auth_id_resp_cd = line.getString(16)
      val acpt_bank = line.getString(17)

      var charge = 0.0
      try {
        charge = line.getDouble(18) / 100
      } catch {
        case e: java.lang.ClassCastException    => charge = 0.0
        case e2: java.lang.NullPointerException => charge = 0.0

      }

      Edge(srcId, dstId, (1, money, charge, isnight, isForeign, card_class, mchnt_tp, trans_chnl, acpt_ins_tp, resp_cd4, acpt_bank, mchnt_cd, term_id, fwd_ins_id_cd, rcv_ins_id_cd))
      //Edge(srcId, dstId, (1, money, charge, isnight, isForeign,    card_class ))
      //Edge(srcId, dstId, (1, money, pdate,trans_hour,isnight, time_detail, region_cd, isForeign, mchnt_tp, mchnt_cd, addrDetail, trans_chnl, term_id, fwd_ins_id_cd, rcv_ins_id_cd, card_class, resp_cd4, acpt_ins_tp, auth_id_resp_cd, acpt_bank, charge))
    }

    var origraph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut) //必须在调用groupEdges之前调用Graph.partitionBy 。

    println("origraph")
//    println("origraph Vertex Num is: " + origraph.numVertices)
//    println("origraph Edge Num is: " + origraph.numEdges)
    //origraph.edges.take(5).foreach(println)
    //    println("need origraph")
    //    origraph.edges.filter(f=>f.attr._4.toInt==1).take(5).foreach(println)

    var mlist_6 = List[String]()
    var mlist_7 = List[String]()
    var mlist_8 = List[String]()
    var mlist_9 = List[String]()
    var mlist_10 = List[String]()

    def most_freq_6(ea: String, eb: String): String = {
      mlist_6 = mlist_6.+:(ea).+:(eb)
      var cnt_pair = mlist_6.map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)((sum, i) => sum + i._2)) //mapValues的用途是直接将Array的内容进行按照key相同的进行统计计算。 也可以写成mapValues(_.foldLeft(0)(_+_._2))  有4个下划线，第一下划线表示数组中的key对应的Array集合，0表示初始值，主要作用也告诉foldLeft函数最后返回Int类型， 第二个下划线表示累加值， _._2中的第一个下划线表示元组
      val most_freq_pair = cnt_pair.toSeq.sortBy(_._2).takeRight(1)(0) //降序
      return most_freq_pair._1
      //     val most_freq_pair = cnt_pair.toSeq.sortWith(_._2 > _._2).take(1):_*  //降序
      //     return most_freq_pair._1
    }

    def most_freq_7(ea: String, eb: String): String = {
      mlist_7 = mlist_7.+:(ea).+:(eb)
      var cnt_pair = mlist_7.map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)((sum, i) => sum + i._2))
      val most_freq_pair = cnt_pair.toSeq.sortBy(_._2).takeRight(1)(0) //降序
      return most_freq_pair._1
    }

    def most_freq_8(ea: String, eb: String): String = {
      mlist_8 = mlist_8.+:(ea).+:(eb)
      var cnt_pair = mlist_8.map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)((sum, i) => sum + i._2))
      val most_freq_pair = cnt_pair.toSeq.sortBy(_._2).takeRight(1)(0) //降序
      return most_freq_pair._1
    }

    def most_freq_9(ea: String, eb: String): String = {
      mlist_9 = mlist_9.+:(ea).+:(eb)
      var cnt_pair = mlist_9.map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)((sum, i) => sum + i._2))
      val most_freq_pair = cnt_pair.toSeq.sortBy(_._2).takeRight(1)(0) //降序
      return most_freq_pair._1
    }

    def most_freq_10(ea: String, eb: String): String = {
      mlist_10 = mlist_10.+:(ea).+:(eb)
      var cnt_pair = mlist_10.map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)((sum, i) => sum + i._2))
      val most_freq_pair = cnt_pair.toSeq.sortBy(_._2).takeRight(1)(0) //降序
      return most_freq_pair._1
    }

    var gset_1 = mutable.Set[String]()
    var gset_2 = mutable.Set[String]()
    var gset_3 = mutable.Set[String]()
    var gset_4 = mutable.Set[String]()
    var gset_5 = mutable.Set[String]()

    def count_distinct_1(ea: String, eb: String): String = { gset_1 = gset_1.+(ea).+(eb); return gset_1.size.toString(); } //如果两条边，那么groupedges 函数就不执行了
    def count_distinct_2(ea: String, eb: String): String = { gset_2 = gset_2.+(ea).+(eb); return gset_2.size.toString(); }
    def count_distinct_3(ea: String, eb: String): String = { gset_3 = gset_3.+(ea).+(eb); return gset_3.size.toString(); }
    def count_distinct_4(ea: String, eb: String): String = { gset_4 = gset_4.+(ea).+(eb); return gset_4.size.toString(); }
    def count_distinct_5(ea: String, eb: String): String = { gset_5 = gset_5.+(ea).+(eb); return gset_5.size.toString(); }

    var graph = origraph.groupEdges((ea, eb) => (ea._1 + eb._1, ea._2 + eb._2, ea._3 + eb._3, ea._4 + eb._4, ea._5 + eb._5,
      most_freq_6(ea._6, ea._6), most_freq_7(ea._7, ea._7), most_freq_8(ea._8, ea._8), most_freq_9(ea._9, ea._9), most_freq_10(ea._10, ea._10),
      count_distinct_1(ea._11, ea._11), count_distinct_2(ea._12, ea._12), count_distinct_3(ea._13, ea._13), count_distinct_4(ea._14, ea._14), count_distinct_5(ea._15, ea._15)))

    println("graph group edges done.")

//    println("graph Vertex Num is: " + graph.numVertices)
//    println("graph Edge Num is: " + graph.numEdges)

    graph = graph.mapEdges(edge =>
      if (edge.attr._1 > 1)
        (edge.attr._1, edge.attr._2, edge.attr._3, edge.attr._4, edge.attr._5, edge.attr._6, edge.attr._7, edge.attr._8, edge.attr._9, edge.attr._10, edge.attr._11, edge.attr._12, edge.attr._13, edge.attr._14, edge.attr._15)
      else
        (edge.attr._1, edge.attr._2, edge.attr._3, edge.attr._4, edge.attr._5, edge.attr._6, edge.attr._7, edge.attr._8, edge.attr._9, edge.attr._10, "1", "1", "1", "1", "1"))

    //    val a = graph.edges.filter(f=>(f.attr._13.toInt>1 & f.attr._13.toInt<5)).take(10) 
    //    a.foreach(println)

    val tempDegGraph = graph.outerJoinVertices(graph.degrees) {
      (vid, encard, DegOpt) => (encard, DegOpt.getOrElse(0))
    }

    //去除边出入度和为2的图      顶点为(顶点名称，度数)
    var coregraph = tempDegGraph.subgraph(epred = triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2)

    println("coregraph done.")

    coregraph = coregraph.outerJoinVertices(coregraph.degrees) {
      (vid, tempProperty, degOpt) => (tempProperty._1, degOpt.getOrElse(0))
    }

    //去除度为0的点
    val degGraph = coregraph.subgraph(vpred = (vid, property) => property._2 != 0).cache

    val cgraph = ConnectedComponents.run(degGraph)
    println("create cgraph in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
    //var cgraph = degGraph.connectedComponents()

    val connectedCount = cgraph.vertices.map(pair => (pair._2, 1)).reduceByKey(_ + _).sortBy(f => f._2, true)
    //connectedCount  (团体, 对应团体规模) 

    println("create connectedCount done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    val ccgraph = degGraph.outerJoinVertices(cgraph.vertices) {
      (vid, tempProperty, connectedOpt) => (tempProperty._1, connectedOpt)
    }
    // ccgraph   (vid,顶点名称，对应团体)

    val ccVertice = ccgraph.vertices.map(line => (line._1, line._2._1, line._2._2.get))

    println("create ccVertice done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    val schemacc = StructType(StructField("cc", LongType, true) :: StructField("count", IntegerType, true) :: Nil)
    val rowRDDcc = connectedCount.map(p => Row(p._1.toLong, p._2)) //原本p._1  是VertexId类型的, Row  不支持这种类型，所以需要转换成Long型
    val cCDF = sqlContext.createDataFrame(rowRDDcc, schemacc)

    val schemacv = StructType(StructField("vid", LongType, true) :: StructField("name", StringType, true) :: StructField("cc", LongType, true) :: Nil)
    val rowRDDcv = ccVertice.map(p => Row(p._1.toLong, p._2, p._3.toLong))
    val cVDF = sqlContext.createDataFrame(rowRDDcv, schemacv)

    //cVDF (vid,顶点名称，对应团体)  join cCDF (对应团体, 团体规模)

    println("create dataframe done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    var joinedDF = cVDF.join(cCDF, cVDF("cc") === cCDF("cc"), "left_outer").drop(cVDF("cc"))
    //joinedDF.show(5) //(vid,顶点名称，对应团体, 规模) 

    val VidconnectedCount = joinedDF.map(row => (row.getLong(0), (row.getString(1), row.getLong(2), row.getInt(3)))) //(vid,顶点名称，对应团体,团体规模)

    println("create VidconnectedCount done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")
    //VidconnectedCount.take(5).foreach(println)

    //VidconnectedCount    RDD(Vid, (顶点名称(即原密卡号)，对应团体, 对应团体规模)) 

    val cCountgraph = coregraph.outerJoinVertices(VidconnectedCount) {
      (vid, oldProperty, vccCountprop) => vccCountprop.getOrElse(("0", 0L, 0)) //这里把顶点名称和度数扔了，只保留   (顶点名称，对应团体,团体规模)
    }
    // cCountgraph 的顶点属性为  (vid,原密卡号，对应团体, 对应团体规模)
    println("create cCountgraph done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    val KLabeledVertices = AlgorithmUtil.KcoresLabel.KLabeledVertices(graph, KMax, sc)

    println("create Kcores done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    val cckgraph = cCountgraph.outerJoinVertices(KLabeledVertices) {
      (vid, oldProperty, kcores) => (oldProperty._1, oldProperty._2, oldProperty._3, kcores.getOrElse(0))
    }

    // cckgraph 的顶点属性为  (vid,原密卡号，对应团体, 对应团体规模, maxKcoreLabel)

    val tempGraph1 = cckgraph.outerJoinVertices(degGraph.inDegrees) {
      (vid, oldProperty, inDegOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, inDegOpt.getOrElse(0))
    }

    //  (vid,原密卡号，对应团体, 对应团体规模，maxKcoreLabel,入度)

    val tempGraph2 = tempGraph1.outerJoinVertices(degGraph.outDegrees) {
      (vid, oldProperty, outDegOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, oldProperty._5, outDegOpt.getOrElse(0))
    }

    //  (vid,原密卡号，对应团体, 对应团体规模，maxKcoreLabel,入度,出度)

    
    degGraph.unpersist(blocking=false)
    
    
    println("create tempGraph2 done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////      
    //3.3 计算转入金额、转出金额、净转账金额
    //3.3.1 转入金额
    val sumInVDF = tempGraph2.aggregateMessages[Double](
      triplet => {
        triplet.sendToDst(triplet.attr._2)
      },
      (a, b) => (a + b)).toDF("vid", "sumIn")

    //3.3.2 转出金额
    val sumOutVDF = tempGraph2.aggregateMessages[Double](
      triplet => {
        triplet.sendToSrc(triplet.attr._2)
      },
      (a, b) => (a + b)).toDF("vid", "sumOut")

    //3.3.4  计算净金额
    var InOutDF = sumInVDF.join(sumOutVDF, sumInVDF("vid") === sumOutVDF("vid"), "left_outer").drop(sumInVDF("vid"))
    //InOutDF.show()
    InOutDF = InOutDF.filter(InOutDF("sumIn").isNotNull && InOutDF("sumOut").isNotNull)
    val pureSum = InOutDF.map(row => (row.getLong(1), (math.abs(row.getDouble(0) - row.getDouble(2)), math.abs(row.getDouble(0) - row.getDouble(2)) / math.abs(row.getDouble(0) + row.getDouble(2)))))

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
    val tempGraph3 = tempGraph2.outerJoinVertices(pureSum) {
      (vid, oldProperty, pureSumOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, oldProperty._5, oldProperty._6, pureSumOpt.getOrElse((9999.0, 9999.0)))
    }

    //  (vid,原密卡号，对应团体, 对应团体规模，maxKcoreLabel,入度,出度, 净流通金额)

    println("create tempGraph3 done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    //统计每个连通图顶点属性
    val ccVerticeRdd = tempGraph3.vertices.map { f =>
      val ccLabel = f._2._2 //团体号
      val vid = f._1 //顶点号,暂时不用
      val vname = f._2._1 //顶点名  ,暂时不用
      val ccCount = f._2._3 //团体规模
      val KcoreLabel = f._2._4
      val BigK = if (KcoreLabel > 4) 1 else 0

      val inDeg = f._2._5
      val outDeg = f._2._6
      val degree = inDeg + outDeg

      val a = f._2._7

      val transNode = if (f._2._7._1 < 500 || f._2._7._2 < 0.01) 1 else 0

      (ccLabel, (ccCount, KcoreLabel, inDeg, outDeg, degree, BigK, transNode))
    }.reduceByKey((r1, r2) => (r1._1, math.max(r1._2, r2._2), math.max(r1._3, r2._3), math.max(r1._4, r2._4), math.max(r1._5, r2._5), r1._6 + r2._6, r1._7 + r2._7))

    //统计每个连通图边的属性  
    var tmpGroupRdd = tempGraph3.triplets.map(f => {
      val ccLabel = f.srcAttr._2 //团体号 
      val transCount = f.attr._1
      val money = f.attr._2
      val charge = f.attr._3
      val nightCount = f.attr._4
      val foreignCount = f.attr._5

      val most_card_class = f.attr._6
      val most_mchnt_tp = f.attr._7
      val most_trans_chnl = f.attr._8
      val most_acpt_ins_tp = f.attr._9
      val most_resp_cd4 = f.attr._10

      val acpt_bank_discnt = f.attr._11
      val mchnt_cd_discnt = f.attr._12
      val term_id_discnt = f.attr._13
      val fwd_ins_id_cd_discnt = f.attr._14
      val rcv_ins_id_cd_discnt = f.attr._15

      (ccLabel, (transCount, money, charge, nightCount, foreignCount, most_card_class, most_mchnt_tp, most_trans_chnl, most_acpt_ins_tp, most_resp_cd4, acpt_bank_discnt, mchnt_cd_discnt, term_id_discnt, fwd_ins_id_cd_discnt, rcv_ins_id_cd_discnt))
    })

    val createCombiner = (v: (Int, Double, Double, Int, Int, String, String, String, String, String, String, String, String, String, String)) => {
      (v._1, v._2, v._3, v._4, v._5, HashSet[String](v._6), HashSet[String](v._7), HashSet[String](v._8), HashSet[String](v._9), HashSet[String](v._10), v._11.toInt, v._12.toInt, v._13.toInt, v._14.toInt, v._15.toInt)
    }

    val mergeValue = (c: (Int, Double, Double, Int, Int, HashSet[String], HashSet[String], HashSet[String], HashSet[String], HashSet[String], Int, Int, Int, Int, Int),
      v: (Int, Double, Double, Int, Int, String, String, String, String, String, String, String, String, String, String)) =>
      {
        (c._1 + v._1, c._2 + v._2, c._3 + v._3, c._4 + v._4, c._5 + v._5,
          c._6 + v._6, c._7 + v._7, c._8 + v._8, c._9 + v._9, c._10 + v._10,
          math.max(c._11, v._11.toInt), math.max(c._12, v._12.toInt), math.max(c._13, v._13.toInt), math.max(c._14, v._14.toInt), math.max(c._15, v._15.toInt))
      }

    val mergeCombiners = (c1: (Int, Double, Double, Int, Int, HashSet[String], HashSet[String], HashSet[String], HashSet[String], HashSet[String], Int, Int, Int, Int, Int),
      c2: (Int, Double, Double, Int, Int, HashSet[String], HashSet[String], HashSet[String], HashSet[String], HashSet[String], Int, Int, Int, Int, Int)) =>
      {
        (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 + c2._5,
          c1._6 union c2._6, c1._7 union c2._7, c1._8 union c2._8, c1._9 union c2._9, c1._10 union c2._10,
          math.max(c1._11, c2._11), math.max(c1._12, c2._12), math.max(c1._13, c2._13), math.max(c1._14, c2._14), math.max(c1._15, c2._15))
      }

    var ccEdgeGroupRdd = tmpGroupRdd.combineByKey(
      createCombiner,
      mergeValue,
      mergeCombiners).map(temp => (temp._1, (temp._2._1, temp._2._2, temp._2._3, temp._2._4, temp._2._5, temp._2._6.size, temp._2._7.size, temp._2._8.size, temp._2._9.size, temp._2._10.size,
        temp._2._11, temp._2._12, temp._2._13, temp._2._14, temp._2._15)))

    var ccEdgeDistDF = tempGraph3.triplets.map(f => {
      val ccLabel = f.srcAttr._2 //团体号     
      val region_cd = f.attr._3
      val mchnt_tp = f.attr._5
      val mchnt_cd = f.attr._6
      val addrDetail = f.attr._7
      (ccLabel, region_cd, mchnt_tp, mchnt_cd, addrDetail)
    }).toDF("label", "region_cd", "mchnt_tp", "mchnt_cd", "addrDetail")

    println("create ccEdgeDistDF done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    val regionCdRdd = ccEdgeDistDF
      .select("label", "region_cd")
      .groupBy('label)
      .agg('label, countDistinct('region_cd))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })
    val mchntTpRdd = ccEdgeDistDF
      .select("label", "mchnt_tp")
      .groupBy('label)
      .agg('label, countDistinct('mchnt_tp))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })
    val mchntCdRdd = ccEdgeDistDF
      .select("label", "mchnt_cd")
      .groupBy('label)
      .agg('label, countDistinct('mchnt_cd))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })
    val addrCdRdd = ccEdgeDistDF
      .select("label", "addrDetail")
      .groupBy('label)
      .agg('label, countDistinct('addrDetail))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })

    println("create addrCdRdd done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    val ccEdgeRdd = ccEdgeGroupRdd.leftOuterJoin(regionCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._2.getOrElse(0)))
    }).leftOuterJoin(mchntTpRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._2.getOrElse(0)))
    }).leftOuterJoin(mchntCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._1._15, f._2._2.getOrElse(0)))
    }).leftOuterJoin(addrCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._1._15, f._2._1._16, f._2._2.getOrElse(0)))
    })

    //ccEdgeRdd.take(5).foreach(println)  

    var ccgraphRdd = ccVerticeRdd.leftOuterJoin(ccEdgeRdd).map(f => {
      val eProp = f._2._2.getOrElse("N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")
      (f._1, f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7,
        eProp._1, eProp._2, eProp._3, eProp._4, eProp._5, eProp._7, eProp._8, eProp._11, eProp._12, eProp._13, eProp._14, eProp._15, eProp._16, eProp._17) //不能超过23个
    })

    ccgraphRdd = ccgraphRdd.filter(f => f._1 != 0L).distinct()
 
    var ccprop_rdd = ccgraphRdd.map { f => (f._1, (f._2, f._3, f._4, f._5, f._6, f._7, f._8, f._9.toString.toDouble, f._10.toString.toDouble, f._11.toString.toDouble, f._12.toString.toDouble, f._13.toString.toDouble, f._14.toString.toDouble, f._15.toString.toDouble, f._16.toString.toDouble, f._17.toString.toDouble, f._18.toString.toDouble, f._19.toString.toDouble, f._20.toString.toDouble, f._21.toString.toDouble, f._22.toString.toDouble)) }
 
    var cc_RDD = cCountgraph.vertices.map(f => (f._2._2, (f._2._1, f._2._3))) //(顶点名称,团体规模)
    var v_rdd = cc_RDD.join(ccprop_rdd)
 
    var vertice_cc = v_rdd.map { f => (f._2._1._1, f._1, f._2._2._1, f._2._2._2, f._2._2._3, f._2._2._4, f._2._2._5, f._2._2._6, f._2._2._7, f._2._2._8, f._2._2._9, f._2._2._10, f._2._2._11, f._2._2._12, f._2._2._13, f._2._2._14, f._2._2._15, f._2._2._16, f._2._2._19, f._2._2._20, f._2._2._21) }
    var vertice_cc_df = vertice_cc.toDF("card_v", "cc", "ccNum", "prop_1", "prop_2", "prop_3", "prop_4", "prop_5", "prop_6", "prop_7", "prop_8", "prop_9", "prop_10", "prop_11", "prop_12", "prop_13", "prop_14", "prop_15", "prop_16", "prop_17", "prop_18")
      
    
    vertice_cc_df = vertice_cc_df.filter(vertice_cc_df("card_v").isNotNull)
    graphata.unpersist(blocking=false)
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    val vertice_cc_cnt = vertice_cc_df.count
     
    println("vertice_cc_df: " + vertice_cc_cnt)
     

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
    
   
      if(vertice_cc_cnt==0)
         stat_DF = stat_DF.join(vertice_cc_df, stat_DF("card") === vertice_cc_df("card_v"), "left_outer") 
          
      stat_DF = stat_DF.na.fill(0)
  
      
      val ListIP1 = List("tot_Counts","tot_Amounts","tot_regions","Night_cnt","tot_large_integer","tot_big1000","tot_cardholder_fails","tot_count_89","tot_term_ids","ccNum","prop_1","prop_4","prop_6","prop_7")
      val ListIP2 = List("tot_Provs","tot_big500","tot_HRloc_trans","tot_abnorm_rate","tot_count_89",
                         "tot_Foreign","transin_counts","transin_amounts","transin_avg","transin_max","distinct_cards_in",
                         "transout_counts","transout_amounts","transout_avg","transout_max","distinct_cards_out",
                         "quxian_counts","quxian_amounts","quxian_avg","quxian_max","consume_counts","consume_amounts","consume_avg","consume_max","daifu_counts","daifu_amounts","daifu_avg","daifu_max")
                         
      val ListIP3 = List("prop_2","prop_3","prop_5","prop_8","prop_9","prop_10","prop_11","prop_12","prop_13","prop_14","prop_15","prop_16","prop_17","prop_18")
      
       
      
      var cur_rank = "sum_rank_0"
      var last_rank = "sum_rank_0"
      
      stat_DF = stat_DF.withColumn(cur_rank, dense_rank.over(Window.partitionBy(lit(1)).orderBy(ListIP1(0))))
      
      var i = 1
      for(item<-ListIP1.drop(1)){ 
        if(stat_DF.columns.contains(item)){
          var rank_col = "rank_" + item 
          stat_DF = stat_DF.withColumn(rank_col, dense_rank.over(Window.partitionBy(lit(1)).orderBy(item)))
          
          cur_rank = "sum_rank_" + i
          stat_DF = stat_DF.withColumn(cur_rank, stat_DF(rank_col) + stat_DF(last_rank))
          last_rank = cur_rank
          i=i+1
          
        }
          
      }
      
      
      val rank_ratio = udf((xin: Long, ratio: Int) => xin.toDouble/ratio.toDouble)
      
      for(item<-ListIP2){ 
        if(stat_DF.columns.contains(item)){
          var rank_col = "rank_" + item 
          stat_DF = stat_DF.withColumn(rank_col, dense_rank.over(Window.partitionBy(lit(1)).orderBy(item)))
          
          cur_rank = "sum_rank_" + i
          stat_DF = stat_DF.withColumn(cur_rank, stat_DF(last_rank) + rank_ratio((stat_DF(rank_col)), lit(2)))
          last_rank = cur_rank
          i=i+1
        }
      }
      
      for(item<-ListIP3){ 
        if(stat_DF.columns.contains(item)){
          var rank_col = "rank_" + item 
          stat_DF = stat_DF.withColumn(rank_col, dense_rank.over(Window.partitionBy(lit(1)).orderBy(item)))
          
          cur_rank = "sum_rank_" + i
          stat_DF = stat_DF.withColumn(cur_rank, stat_DF(last_rank) + rank_ratio((stat_DF(rank_col)), lit(4)))
          last_rank = cur_rank
          i=i+1
        }
      }
       
      
      println("Rank done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.") 
      
      stat_DF = stat_DF.join(MD5PairDF, stat_DF("card") === MD5PairDF("md5card"), "right_outer") 
      //stat_DF.show()
       
      val last_DF = stat_DF.select("oricard", cur_rank).coalesce(1).sort(desc(cur_rank))
      println("last_DF:")
      last_DF.show(200)
      //last_DF.rdd.map(_.mkString(",")).coalesce(1).saveAsTextFile(outputFile)
      last_DF.rdd.map(_.mkString(",")).saveAsTextFile(outputFile)
       
    
    
    
    
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