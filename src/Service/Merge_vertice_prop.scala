package Service

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
 
import SparkContext._ 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import AlgorithmUtil._
import scala.reflect.ClassTag
import scala.collection.mutable
import scala.collection.mutable.HashSet

object Merge_vertice_prop { 
   private val startDate = "20170101"
   private val endDate = "20170101"
   private val KMax = 10
   
  def any_to_double[T: ClassTag](b: T):Double={
    if(b==true)
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

    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           //s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.queue2")
           
    var data = hc.sql(
      s"select pri_acct_no_conv," +
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
      s"cast(total_disc_at as double) " +
      s"tfr_out_acct_no, " +
      s"tfr_in_acct_no " +
      s"from tbl_common_his_trans where " +
      s"pdate>=$startDate and pdate<=$endDate")
        .toDF("card","money","date","time","region_cd","trans_md","mchnt_tp","mchnt_cd","trans_chnl",
              "term_id","fwd_ins_id_cd","rcv_ins_id_cd","card_class","resp_cd4","acpt_ins_tp","auth_id_resp_cd","acpt_bank", "charge","srccard","dstcard")
        .repartition(100)
  
     println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
      
    
/////////////////////////////////////交易标记//////////////////////////////////////////         
    val get_hour = udf[Int, String]{xstr => xstr.substring(0,2).toInt }
    data = data.withColumn("hour", get_hour(data("time")))
    
     println("is_Night")
    val is_Night = udf[Double, String]{xstr => 
      val h = xstr.toInt
      val night_list = List(23,0,1,2,3,4,5)
      any_to_double(night_list.contains(h))
    }
    
    data = data.withColumn("is_Night", is_Night(data("hour")))
    
    
    
    val getProvince = udf[String, String]{xstr => xstr.substring(0,2)}
    data = data.withColumn("Prov", getProvince(data("region_cd")))
    
    
    
     val getRMB = udf[Long, String]{xstr => (xstr.toDouble/100).toLong}
     data = data.withColumn("RMB", getRMB(data("trans_at")))
     
     val is_RMB_500 = udf[Double, Long]{xstr => any_to_double(xstr.toDouble%500 == 0)}
    data = data.withColumn("is_bigRMB_500", is_RMB_500(data("RMB")))
    
    val is_RMB_1000 = udf[Double, Long]{xstr => any_to_double(xstr.toDouble%1000 == 0)}
    data = data.withColumn("is_bigRMB_1000", is_RMB_1000(data("RMB")))
    
     //println("智策大额整额定义")
    println("is_large_integer")
    val is_large_integer = udf[Double, Long]{a =>
      val b = a.toString.size
      val c = a.toDouble/(math.pow(10, (b-1)))
      val d = math.abs(c-math.round(c))
      val e = d.toDouble/b.toDouble
      any_to_double(e<0.01 && a>1000)
    } 
    data = data.withColumn("is_large_integer", is_large_integer(data("RMB")))

     //println("交易金额中8和9的个数")
    println("count_89")
    val count_89 = udf[Double, String]{xstr =>
      var cnt = 0
      xstr.foreach{x => if(x=='8' || x=='9') cnt = cnt+1 }
      cnt.toDouble
    }    
    data = data.withColumn("count_89", count_89(data("trans_at")))
    
    
    println("is_highrisk_loc")
    val HighRisk_Loc = List("1425","4050","4338","5624","5923","6123","6431","3974",   //电信诈骗
        "5810","5972","5880","6054","6582","6852","6851","6762","7035","7095","6314","6366","6266","6927","8991","7517","7580","7517","3371","3336","3724","7975","7910","5119","5118","8737","8360","6125","5625","7035","6754","6900","7155","7096","6574","6761","6717","7091","5546","6623","7039","6755","5654","6900","6900","5210","7348")  //涉毒
    val is_highrisk_loc = udf[Double, String]{xstr => any_to_double(HighRisk_Loc.contains(xstr.substring(0,2)))}    
    data = data.withColumn("is_highrisk_loc", is_highrisk_loc(data("acpt_ins_id_cd_RG")))
     
     //println("持卡人原因导致的失败交易")
    println("is_cardholder_fail")
    val cardholder_fail = udf[Double, String]{xstr => any_to_double(List("51","55","61","65","75").contains(xstr))}    
    data = data.withColumn("cardholder_fail", cardholder_fail(data("resp_cd4")))
    
    //println("是否正常汇率")
    println("not_norm_rate")                 
    val not_norm_rate = udf[Double, String]{xstr => any_to_double(xstr!="30001000" && xstr!="61000000")}    
    data = data.withColumn("not_norm_rate", not_norm_rate(data("fwd_settle_conv_rt")))
    
    
    val isForeign = udf[Double, String]{xstr => any_to_double(!xstr.equals("1"))}   
    data = data.withColumn("isForeign", isForeign(data("trans_md")))
    
    
/////////////////////////////////////////////////////////////////////////////////////////////////////     
    
////////////////////////整体group///////////////////////////////    
    val tot_agg = data.groupBy("card")
    var stat_DF = tot_agg.agg(sum("is_Night") as "Night_cnt") 
         
    var tmp_DF = tot_agg.agg(countDistinct("region_cd") as "tot_regions")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_regions"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(countDistinct("term_id") as "tot_term_ids")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_term_ids"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
       
    tmp_DF = tot_agg.agg(countDistinct("Prov") as "tot_Provs")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_Provs"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("is_bigRMB_500") as "tot_big500")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_big500"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("is_bigRMB_1000") as "tot_big1000")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_big1000"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("is_large_integer") as "tot_large_integer")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_large_integer"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
     
    tmp_DF = tot_agg.agg(sum("is_highrisk_loc") as "tot_HRloc_trans")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_HRloc_trans"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("is_cardholder_fail") as "tot_cardholder_fails")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_cardholder_fails"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("not_norm_rate") as "tot_abnorm_rate")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_abnorm_rate"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("count_89") as "tot_count_89")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_count_89"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = tot_agg.agg(sum("isForeign") as "tot_Foreign")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("tot_Foreign"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    
    
/////////////////////////////////////////////////////////////////////////////////////////////////////         
    
     var transferList = sc.textFile("").collect
      
     var transinData = data.filter(data("card").isin(transferList : _*) && data("trans_id").===("S33"))
     var transoutData = data.filter(data("card").isin(transferList : _*) && data("trans_id").===("S25"))
     var quxianData = data.filter(data("card").isin(transferList : _*) && data("trans_id").===("S24"))
     var queryData = data.filter(data("card").isin(transferList : _*) && data("trans_id").===("S00"))
     var consumeData = data.filter(data("card").isin(transferList : _*) && data("trans_id").===("S22"))
     
     
     val transin_agg = transinData.groupBy("card")
     val transout_agg = transoutData.groupBy("card")  
     val quxian_agg = quxianData.groupBy("card") 
     val query_agg = queryData.groupBy("card")  
     val consume_agg = consumeData.groupBy("card") 
 

     ////////////////////////整体group///////////////////////////////    
    tmp_DF = transinData.agg(count("RMB") as "transin_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
     
    tmp_DF = transinData.agg(sum("RMB") as "transin_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transinData.agg(avg("RMB") as "transin_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transinData.agg(avg("RMB") as "transin_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transin_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transinData.agg(countDistinct("srccard") as "distinct_cards_in") 
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("distinct_cards_in"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transinData.agg(countDistinct("srccard") as "distinct_cards_in") 
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("distinct_cards_in"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
     
    
    
    tmp_DF = transoutData.agg(count("RMB") as "transout_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
     
    tmp_DF = transoutData.agg(sum("RMB") as "transout_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transoutData.agg(avg("RMB") as "transout_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transoutData.agg(avg("RMB") as "transout_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("transout_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = transinData.agg(countDistinct("dstcard") as "distinct_cards_out") 
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("distinct_cards_out"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
 
    
    
    tmp_DF = quxianData.agg(count("RMB") as "quxian_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
     
    tmp_DF = quxianData.agg(sum("RMB") as "quxian_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = quxianData.agg(avg("RMB") as "quxian_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = quxianData.agg(avg("RMB") as "quxian_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("quxian_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    
    tmp_DF = consumeData.agg(count("RMB") as "consume_counts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_counts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
     
    tmp_DF = consumeData.agg(sum("RMB") as "consume_amounts")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_amounts"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = consumeData.agg(avg("RMB") as "consume_avg")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
    
    tmp_DF = consumeData.agg(avg("RMB") as "consume_max")
    tmp_DF = tmp_DF.select(tmp_DF("card").as("card_2"), tmp_DF("consume_avg"))
    stat_DF = stat_DF.join(tmp_DF, stat_DF("card")===tmp_DF("card_2"), "left_outer").drop("card_2")
/////////////////////////////////////////////////////////////////////////////////////////////////////
     
     //hive> select pri_acct_no_conv, tfr_in_acct_no, tfr_out_acct_no from tbl_common_his_trans where trans_id='S33' and pdate='20160101' limit 5;
     //9533b3a55d687fb985709159482dc0aa        9533b3a55d687fb985709159482dc0aa        7175dab3e2a246f6d028e737ec70fae1
     //select pri_acct_no_conv, tfr_in_acct_no, tfr_out_acct_no from tbl_common_his_trans where trans_id='S25' and pdate='20160101' limit 5;
     //2f935e17602ebf67fea2aa7cbf76d1c8        ba7dc348523e67d5b0d841d19650ccf0        2f935e17602ebf67fea2aa7cbf76d1c8
  
          
    sc.stop()
    
  } 
  
  
  

  
} 