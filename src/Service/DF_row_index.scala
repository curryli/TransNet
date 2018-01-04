package Service
//为dataframe 添加行索引
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

object DF_row_index {
  private val startDate = "20170511"
  private val endDate = "20170511"
  private val KMax = 8
  private val top_CCs = 100
  private val top_vs = 100
  
   

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

    val sparkConf = new SparkConf().setAppName("transGraph")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    val startTime = System.currentTimeMillis();

    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
      s"set mapred.max.split.size=10240000000" +
      s"set mapred.min.split.size.per.node=10240000000" +
      s"set mapred.min.split.size.per.rack=10240000000" +
      s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
      s"set mapreduce.job.queuename=root.queue2")

      
//    val seedList = Array("66a6290d0a7ecf4d3fb7e4e4a04e0ebb",
//                         "5087ce168109c1adb5ba75c940ce942e",
//                         "bc5aafb5f7dc4036d681c48f5fae3cc4",
//                         "7de36e2a2f19fb82d8717ddfd80968d7",
//                         "c0e2e62b0d177caf2cef9e6c660357f7",
//                         "7cdb1de5756b3e506cdef4816316644a")
      
                         
    val seedList = Array("171785e888ac86a2e9fb63cb8f4e6618","3c94b8affa91fc5d983ef4aa8d439e67","514b2c52e50b742ae747290142e05d2b","766f8c698545b7ec70f57492df4ec801","6d9ca24a3651459aa0a26b0e5d005a47","1c7519a1e6b78b10a0ef6c634c2a00fe","313b03451a78a57f777e9d7c95978959","7ac9ba5fcbc6af2a2817d1383b2f3899","3521edbd2cca3d8ee4e9f7779b10e1fe","e8e41e00dd869204d074e287b0016945","c7718d30da2e54ae56cbec553a90db22","d4148edab0f4e3bf66de5ecbceaf315b","264cc7054b06fc86eb93408592866aaf","8a6164f4fa2fc4604953d068493ab613","f691e1701f1f3c7c575bacd40612ab85","f2812c09bec7750b73567b1c375205db","604c8c1ba9121012d886e13ac1f3cd15","36a7543d957044e4cc43d7353309c0ef","7d73a25a4341ad024a825c0c68355dda","1a8135d3da9e54a74db5e0cddf51b68d","372daaf9a9dd3703dccc8b9dcabee859","baabd7120c11b1de09d4dd805e79c82b","9c4c56a01c1a004eb94d3362efd3b429","158396d3631a06124cab6ac0a6983894","1f1e4f8ae18d4edf8c4a80ad39a12acb","37328ac374f2fb4b7ac42ec8f855cae1","6e9059ca282b5b4991eb92aab548e0a6","d03a150e22032f66abae63334c47a9af","591aa38b74db589e51c335d9edac3865","2e2019c2394bb9ff329af75183da433c","516d6860691abadfcaabf4f2762bd695","9908e10d5559a902168e0acd683b1950","f44b4d4905a991c62310466262fa48e1","2ef1f5af2d0b00025c6959e0d3f4d106","2d351643424ba6dc16102ddb0a126409","edcfbb9878b4774efd3a37653cbf9bb1","2c1f3de65853f1bf545a639d8ab398c7","80b068834d8d4b7b89e7d681cfdecb0a","2e17f7e55fbe4f8fdf1e921ce9059a09","752291281ec8edaad0a35c012a9015ad") 
      
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
        s"fwd_settle_conv_rt, " +
        s"trans_id, " +
        s"pri_acct_no_conv " +
        s"from tbl_common_his_trans where " +
        s"pdate>=$startDate and pdate<=$endDate ")
      .toDF("srccard", "dstcard", "money", "date", "time", "region_cd", "trans_md", "mchnt_tp", "mchnt_cd", "trans_chnl",
        "term_id", "fwd_ins_id_cd", "rcv_ins_id_cd", "card_class", "resp_cd4", "acpt_ins_tp", "auth_id_resp_cd", "acpt_bank", "charge", "fwd_settle_conv_rt", "trans_id", "card")
      .repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER)
 
 
      
    var Useddata = Alldata.filter(Alldata("card").isin(seedList: _*)).repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER)
      
//    var transferList = antiSearch(Alldata,startDate,endDate,seedList)    
//    var graphata =  Alldata.filter(Alldata("srccard").isin(transferList : _*) or Alldata("dstcard").isin(transferList : _*))
  
    var graphata =  Alldata.filter(Alldata("srccard").isin(seedList : _*) or Alldata("dstcard").isin(seedList : _*))
     
     
    Alldata.unpersist(blocking = false)
    
     
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

    //println("是否正常汇率")
    println("not_norm_rate")
    val not_norm_rate = udf[Double, String] { xstr => any_to_double(xstr != "30001000" && xstr != "61000000") }
    Useddata = Useddata.withColumn("not_norm_rate", not_norm_rate(Useddata("fwd_settle_conv_rt")))

    val isForeign = udf[Double, String] { xstr => any_to_double(!xstr.equals("1")) }
    Useddata = Useddata.withColumn("isForeign", isForeign(Useddata("trans_md")))

    /////////////////////////////////////////////////////////////////////////////////////////////////////     

    ////////////////////////整体group///////////////////////////////    
    val tot_agg = Useddata.groupBy("card")
    var stat_DF = tot_agg.agg(sum("is_Night") as "Night_cnt")

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

    val transin_gb = transinData.groupBy("card")
    val transout_gb = transoutData.groupBy("card")
    val quxian_gb = quxianData.groupBy("card")
    val query_gb = queryData.groupBy("card")
    val consume_gb = consumeData.groupBy("card")

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
 
    stat_DF.show()
      
    stat_DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    stat_DF.show()
    
    //    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//    stat_DF = stat_DF.join(vertice_cc_df, stat_DF("card") === vertice_cc_df("card"), "left_outer").drop(stat_DF("card"))
//    stat_DF.show() 
////    stat_DF = stat_DF.sort("tot_Counts","tot_Amounts","tot_regions","Night_cnt","tot_large_integer","tot_term_ids","ccNum","prop_1","prop_4","prop_6","prop_7",
////        "tot_big1000","tot_Provs","tot_big500","tot_HRloc_trans","tot_cardholder_fails","tot_abnorm_rate","tot_count_89",
////        "tot_Foreign","transin_counts","transin_amounts","transin_avg","transin_max","distinct_cards_in",
////        "transout_counts","transout_amounts","transout_avg","transout_max","distinct_cards_out",
////        "quxian_counts","quxian_amounts","quxian_avg","quxian_max","consume_counts","consume_amounts","consume_avg","consume_max",
////        "prop_2","prop_3","prop_5","prop_8","prop_9","prop_10","prop_11","prop_12","prop_13","prop_14","prop_15","prop_16","prop_17","prop_18")
//    
//    
//    val ListIP1 = List("tot_Counts","tot_Amounts","tot_regions","Night_cnt","tot_large_integer","tot_term_ids","ccNum","prop_1","prop_4","prop_6","prop_7")
//    val ListIP2 = List("tot_big1000","tot_Provs","tot_big500","tot_HRloc_trans","tot_cardholder_fails","tot_abnorm_rate","tot_count_89",
//                       "tot_Foreign","transin_counts","transin_amounts","transin_avg","transin_max","distinct_cards_in",
//                       "transout_counts","transout_amounts","transout_avg","transout_max","distinct_cards_out",
//                       "quxian_counts","quxian_amounts","quxian_avg","quxian_max","consume_counts","consume_amounts","consume_avg","consume_max")
//                       
//    val ListIP3 = List("prop_2","prop_3","prop_5","prop_8","prop_9","prop_10","prop_11","prop_12","prop_13","prop_14","prop_15","prop_16","prop_17","prop_18")
//    
//    import scala.collection.immutable.Range
//    
////    for(item<-ListIP1){
////      var temp_DF = stat_DF.sort(item)
////      val temp_list = temp_DF.coalesce(1).map{r => r.getString(0)}.collect()
////      //val temp_pair = temp_list.zip(List.range(0,temp_list.length))
////      val temp_pair = temp_list.zipWithIndex
////    }
//    
//    for(item<-ListIP1){
//      var temp_DF = stat_DF.sort(item)
//      temp_DF = temp_DF.select("card","item")
//      temp_DF.withColumn("ranks", row_number)
//      
//    }
    
   //方法1
    stat_DF = stat_DF.withColumn("ranks", row_number.over(Window.partitionBy(lit(1)).orderBy(lit(1))))
    //Note that I use lit(1) for both the partitioning and the ordering -- this makes everything be in the same partition, and seems to preserve the original ordering of the DataFrame, but I suppose it is what slows it way down.
    //lit 函数的用法见 https://yq.aliyun.com/articles/43588
    
    stat_DF.show
    
    //方法2
    stat_DF = dfZipWithIndex(stat_DF)  //比 row_number.over(Window.partitionBy(lit(1)).orderBy(lit(1))要快
    stat_DF.show
    
    println("All flow done in " + (System.currentTimeMillis() - startTime) / (1000 * 60) + " minutes.")

    sc.stop()

  }

  
  
  
  
  
  def antiSearch(Alldata: DataFrame, beginDate: String, endDate: String, seedList: Array[String]) = {
      val maxitertimes =5
      var currentDataSize = seedList.length.toLong
 
      var lastDataSize=0L
  
      var tempData=null 
      var cardList=seedList
 
      var i=0
      
      while(i<maxitertimes && lastDataSize!=currentDataSize){
         i=i+1
         println("Start iteration " + i)
         var tempData= Alldata.filter(Alldata("srccard").isin(cardList: _*) or Alldata("dstcard").isin(cardList: _*)).select(s"srccard",s"dstcard").distinct()    
         var dataFrame1=tempData.select(s"srccard").distinct()
         var dataFrame2=tempData.select(s"dstcard").distinct() 
         var temp= dataFrame1.unionAll(dataFrame2).distinct().map{r => r.getString(0)}
         cardList=temp.collect()  

         lastDataSize=currentDataSize
         currentDataSize=temp.count()
         println("currentDataSize: ", currentDataSize)

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