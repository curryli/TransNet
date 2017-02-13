package FraudFlow

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

object StaticForConnected { 
   private val startDate = "20160707"
   private val endDate = "20160707"
   private val KMax = 10
   
  
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

   
           
    val data = hc.sql(
      s"select tfr_in_acct_no," +
      s"tfr_out_acct_no, " +
      s"fwd_settle_at, " +
      s"hp_settle_dt, " +
      s"loc_trans_tm, " +
      s"substring(acpt_ins_id_cd,-4,4) as region_cd, " +
      s"trim(trans_md), " +
      s"mchnt_tp, " +
      s"mchnt_cd, " +
      s"card_accptr_nm_addr, " +
      s"loc_trans_tm, " +          //重复
      s"cast(total_disc_at as int) " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt>=$startDate and hp_settle_dt<=$endDate and trans_id ='S33' ")
        .toDF("srccard","dstcard","money","date","time","region_cd","trans_md","mchnt_tp","mchnt_cd","card_accptr_nm_addr", "loc_trans_tm", "total_disc_at")
        .repartition(100)
  
    data.show(5)
        
    println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
        
    import sqlContext.implicits._   
    val InPairRdd = data.map(line => (HashEncode.HashMD5(line.getString(0)), line.getString(0)))                
    val OutPairRdd = data.map(line => (HashEncode.HashMD5(line.getString(1)), line.getString(1)))      
    val verticeRDD = InPairRdd.union(OutPairRdd).distinct()
    
    //println(verticeRDD.count())
    
      val edgeRDD = data.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(1))
        val money = line.getLong(2)
        val region_cd = line.getString(5)
        val isForeign = if(line.getString(6).equals("2")) 1 else 0  // 异地交易次数+1            交易模式    1 同城   2 异地    4 跨境
        val mchnt_tp = line.getString(7)
        val mchnt_cd =  line.getString(8)
        val addrDetail = line.getString(9)  
        val trans_hour = line.getString(10).substring(0, 2).toInt
        val isnight =  if(trans_hour>0 && trans_hour<6) 1 else 0  //是否夜间交易  1是 0否
        val charge = line.getInt(11)
        Edge(srcId, dstId, (1, money, region_cd, isForeign, mchnt_tp, mchnt_cd, addrDetail, isnight, charge))
    }
    
    var origraph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
    
//    println("origraph")
//    origraph.edges.take(5).foreach(println)
//    println("need origraph")
//    origraph.edges.filter(f=>f.attr._4.toInt==1).take(5).foreach(println)
    
    var graph = origraph.groupEdges((ea,eb) => (ea._1+eb._1, ea._2+eb._2, ea._3, ea._4+eb._4, ea._5, ea._6, ea._7, ea._8+eb._8, ea._9+eb._9))    //每条边是  两个账号之间的转账关系，权重是（总次数,总金额， 转账地址，异地交易次数，商户类型，商户代码，详细地址，跨境交易次数）
    
//    println("graph")
//    graph.edges.take(5).foreach(println)
//    println("need1 graph")
//    graph.edges.filter(f=>f.attr._4.toInt>1).take(5).foreach(println)
//    println("need2 graph")
//    graph.edges.filter(f=>f.attr._8.toInt>1).take(5).foreach(println)
    
    
    val tempDegGraph = graph.outerJoinVertices(graph.degrees){
      (vid, encard, DegOpt) => (encard, DegOpt.getOrElse(0))
    }
     
     //去除边出入度和为2的图
    var coregraph = tempDegGraph.subgraph(epred = triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2) 
 
    coregraph = coregraph.outerJoinVertices(coregraph.degrees){
       (vid, tempProperty, degOpt) => (tempProperty._1, degOpt.getOrElse(0))
    } 
    
    //去除度为0的点
    val degGraph = coregraph.subgraph(vpred = (vid, property) => property._2!=0).cache
    
    val cgraph = ConnectedComponents.run(degGraph)
    println("create cgraph in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )   
    //var cgraph = degGraph.connectedComponents()
    
    
    val connectedCount = cgraph.vertices.map(pair=>(pair._2, 1)).reduceByKey(_+_).sortBy(f => f._2, true)
    //connectedCount  (团体, 对应团体规模) 
    
    println("create connectedCount done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val ccgraph = degGraph.outerJoinVertices(cgraph.vertices){
      (vid, tempProperty, connectedOpt) => (tempProperty._1, connectedOpt)
    }
    // ccgraph   (vid,顶点名称，对应团体)
    
    val ccVertice = ccgraph.vertices.map(line => (line._1, line._2._1, line._2._2.get))
    
    println("create ccVertice done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val schemacc = StructType(StructField("cc",LongType,true)::StructField("count",IntegerType,true):: Nil)
    val rowRDDcc = connectedCount.map(p=>Row(p._1.toLong, p._2))     //原本p._1  是VertexId类型的, Row  不支持这种类型，所以需要转换成Long型
    val cCDF = sqlContext.createDataFrame(rowRDDcc, schemacc)
    
    val schemacv = StructType(StructField("vid",LongType,true)::StructField("name",StringType,true)::StructField("cc",LongType,true)::Nil)
    val rowRDDcv = ccVertice.map(p=>Row(p._1.toLong,p._2,p._3.toLong))
    val cVDF = sqlContext.createDataFrame(rowRDDcv, schemacv)
   
    //cVDF (vid,顶点名称，对应团体)  join cCDF (对应团体, 团体规模)
    
    println("create dataframe done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    var joinedDF = cVDF.join(cCDF, cVDF("cc") === cCDF("cc"), "left_outer").drop(cVDF("cc"))
    joinedDF.show(5)  //(vid,顶点名称，对应团体, 规模) 

    val VidconnectedCount = joinedDF.map(row=>(row.getLong(0), (row.getString(1),row.getLong(2),row.getInt(3))))

    println("create VidconnectedCount done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    VidconnectedCount.take(5).foreach(println)
    
    //VidconnectedCount    RDD(Vid, (顶点名称(即原密卡号)，对应团体, 对应团体规模)) 

    val cCountgraph = coregraph.outerJoinVertices(VidconnectedCount){
      (vid, oldProperty, vccCountprop) => vccCountprop.getOrElse(("0",0L,0))
    }
     // cCountgraph 的顶点属性为  (vid,原密卡号，对应团体, 对应团体规模)
    println("create cCountgraph done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
  
    val KLabeledVertices = AlgorithmUtil.KcoresLabel.KLabeledVertices(graph, KMax, sc)
 
    println("create Kcores done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val cckgraph = cCountgraph.outerJoinVertices(KLabeledVertices){
      (vid, oldProperty, kcores) => (oldProperty._1, oldProperty._2, oldProperty._3, kcores.getOrElse(0)) 
    }
    
    // cckgraph 的顶点属性为  (vid,原密卡号，对应团体, 对应团体规模, maxKcoreLabel)
 
    val tempGraph1 = cckgraph.outerJoinVertices(degGraph.inDegrees){
      (vid, oldProperty, inDegOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, inDegOpt.getOrElse(0)) 
    }    
    
    //  (vid,原密卡号，对应团体, 对应团体规模，maxKcoreLabel,入度)

    
    val tempGraph2 = tempGraph1.outerJoinVertices(degGraph.outDegrees){
      (vid, oldProperty, outDegOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, oldProperty._5, outDegOpt.getOrElse(0)) 
    }    
    
    //  (vid,原密卡号，对应团体, 对应团体规模，maxKcoreLabel,入度,出度)
    
    println("create tempGraph2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////      
    //3.3 计算转入金额、转出金额、净转账金额
    //3.3.1 转入金额
    val sumInVDF = tempGraph2.aggregateMessages[Long](
      triplet => {
        triplet.sendToDst(triplet.attr._2)
      },
      (a, b) => (a + b)).toDF("vid","sumIn")

    //3.3.2 转出金额
    val sumOutVDF = tempGraph2.aggregateMessages[Long](
      triplet => {
        triplet.sendToSrc(triplet.attr._2)
      },
      (a, b) => (a + b)).toDF("vid","sumOut")

  
    //3.3.4  计算净金额
    var InOutDF = sumInVDF.join(sumOutVDF, sumInVDF("vid") === sumOutVDF("vid"), "left_outer").drop(sumInVDF("vid"))
    InOutDF.show(10)

//有的节点可能只有sumIn  有的节点可能只有sumOut，这些对我们没用，我们只要统计过渡节点，即既有转出又有转入而且金额差不多的。
//+--------+----------+-------+
//|   sumIn|       vid| sumOut|
//+--------+----------+-------+
//| 2210000|      null|   null|
//| 1219200|      null|   null|
//|  500000|1072200631|4000000|
//|  520000|      null|   null|
//| 5000000|      null|   null|
//| 2800000|      null|   null|
//| 1100000|      null|   null|
//|  200000|      null|   null|
//|10230000|2795824927|3890000|
//|  140000|      null|   null|
//+--------+----------+-------+

    InOutDF = InOutDF.filter(InOutDF("sumIn").isNotNull &&  InOutDF("sumOut").isNotNull )
    val pureSum = InOutDF.map(row=>(row.getLong(1), (math.abs(row.getLong(0)-row.getLong(2)), math.abs(row.getLong(0)-row.getLong(2))/math.abs(row.getLong(0)+row.getLong(2)))))
    
 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
    
    
    val tempGraph3 = tempGraph2.outerJoinVertices(pureSum){
      (vid, oldProperty, pureSumOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, oldProperty._5, oldProperty._6,  pureSumOpt.getOrElse((9999L,9999L))) 
    }    
    
    //  (vid,原密卡号，对应团体, 对应团体规模，maxKcoreLabel,入度,出度, 净流通金额)
    
    println("create tempGraph3 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    
    //统计每个连通图顶点属性
    val ccVerticeRdd = tempGraph3.vertices.map{f =>
      val ccLabel = f._2._2   //团体号
      val vid = f._1  //顶点号,暂时不用
      val vname = f._2._1  //顶点名  ,暂时不用
      val ccCount = f._2._3  //团体规模
      val KcoreLabel = f._2._4   
      val BigK = if(KcoreLabel>4) 1 else 0

      val inDeg = f._2._5
      val outDeg = f._2._6
      val degree = inDeg + outDeg
      
      val transNode = if(f._2._7._1.get < 500 || f._2._7._2.get < 0.01 ) 1 else 0 
 
      
      (ccLabel, (ccCount, KcoreLabel, inDeg, outDeg, degree, BigK, transNode))
    }.reduceByKey((r1, r2) => (r1._1, math.max(r1._2, r2._2), math.max(r1._3, r2._3), math.max(r1._4, r2._4), math.max(r1._5, r2._5), r1._6+r2._6, r1._7+r2._7))
    
   //统计每个连通图边的属性  
    var ccEdgeGroupRdd = tempGraph3.triplets.map(f => {
      val ccLabel = f.srcAttr._2   //团体号 
      val transCount = f.attr._1
      val money = f.attr._2
      val foreignCount = f.attr._4
      val nightCount = f.attr._8
      val charge = f.attr._9
//      val region_cd = f.attr._3
//      val mchnt_tp = f.attr._5
//      val mchnt_cd = f.attr._6
//      val addrDetail = f.attr._7  
      (ccLabel, (money, transCount, foreignCount, nightCount,charge))
    }).reduceByKey((e1, e2) => (e1._1+e2._1, e1._2+e2._2, e1._3+e2._3, e1._4+e2._4, e1._5+e2._5))

    var ccEdgeDistDF = tempGraph3.triplets.map(f => {
      val ccLabel = f.srcAttr._2   //团体号     
      val region_cd = f.attr._3
      val mchnt_tp = f.attr._5
      val mchnt_cd = f.attr._6
      val addrDetail = f.attr._7  
      (ccLabel, region_cd, mchnt_tp, mchnt_cd, addrDetail)
    }).toDF("label", "region_cd", "mchnt_tp", "mchnt_cd", "addrDetail")
    
     println("create ccEdgeDistDF done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")

    
    
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
    
    println("create addrCdRdd done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  
      
    val ccEdgeRdd = ccEdgeGroupRdd.leftOuterJoin(regionCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3,f._2._1._4, f._2._1._5, f._2._2.getOrElse(0)))
    }).leftOuterJoin(mchntTpRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._2.getOrElse(0)))
    }).leftOuterJoin(mchntCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._2.getOrElse(0)))
    }).leftOuterJoin(addrCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._2.getOrElse(0)))
    })  
    
    ccEdgeRdd.take(5).foreach(println)  
    
    var ccgraphRdd = ccVerticeRdd.leftOuterJoin(ccEdgeRdd).map(f => {
      val eProp = f._2._2.getOrElse("N","N","N","N","N","N","N","N","N")
      (f._1, f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, 
        eProp._1, eProp._2, eProp._3, eProp._4, eProp._5, eProp._6, eProp._7, eProp._8, eProp._9)
    }) 
     
    ccgraphRdd = ccgraphRdd.filter(f=>f._1 != 0L)
    
    //ccLabel, 团体规模, 最大K, 最大入度, 最大出度, 最大度, BigK数目, 过渡节点数
    // 总交易金额, 总交易次数, 总异地交易次数, 总跨境交易次数，总手续费， 交易省市总数，商户类型总数，商户代码总数，交易地点总数
    
//ccLabel,ccNum, maxK, maxInDeg, maxOutDeg, maxDeg, BigKNum, TransNum, totalMoney, totalTransCount, foreignCount, nightCount, charge, regionCount, mchnttpCount, mchntcdCount, addrDetailCount

    println("ccgraphRdd created in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    ccgraphRdd.take(5).foreach(println)
    
    ccgraphRdd.saveAsTextFile("xrli/TeleFraud/" + startDate + "_" + (endDate.toLong-startDate.toLong).toString()  + "/graphProp")
     
    println("All flow done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    

    
    sc.stop()
    
  } 
  
  
  

  
} 