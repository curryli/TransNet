package MultiEvent

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

object StaticGraph { 

    
  def main(args: Array[String]) { 
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.INFO);
    Logger.getLogger("akka").setLevel(Level.INFO);
    Logger.getLogger("hive").setLevel(Level.INFO);
    Logger.getLogger("parse").setLevel(Level.INFO);
    
    val sparkConf = new SparkConf().setAppName("MultiCompute")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)  
  
    val startTime = System.currentTimeMillis(); 
    
    val SQLdata = MultiUtil.getFromSQL(hc)
    
    val transdata = SQLdata(0)
    val quxiandata = SQLdata(1)
    val querydata = SQLdata(2)
    val consumedata = SQLdata(3)
    
    import sqlContext.implicits._   
      val InPairRdd = transdata.map(line => line.getString(0))                
      val OutPairRdd = transdata.map(line => line.getString(1)) 
      val QXcardRdd = quxiandata.map(line => line.getString(0)) 
      val QYcardRdd = querydata.map(line => line.getString(0))
      val consumeRdd = consumedata.map(line => line.getString(0))
    
    
    val cardRdd = InPairRdd.union(OutPairRdd).union(QXcardRdd).union(QYcardRdd).union(consumeRdd).distinct().map{line => 
      val vertexType = "card"
      val card = new MultiUtil.card_VP(line.trim)
      val region = new MultiUtil.region_VP("")
      val mchnt_cd = new MultiUtil.mchnt_cd_VP("")
      val term_id = new MultiUtil.term_id_VP("")
      val Vertex_Property = new MultiUtil.Vertex_Property_Class(vertexType,card,region,mchnt_cd,term_id)
      (HashEncode.HashMD5(line.trim), Vertex_Property)
      }
    
    val transregionRdd = transdata.map(line => line.getString(5)) 
    val QXregionRdd = quxiandata.map(line => line.getString(4)) 
    val QYregionRdd = querydata.map(line => line.getString(3)) 
    val consume_regionRdd = consumedata.map(line => line.getString(4))
    val regionRdd = transregionRdd.union(QXregionRdd).union(QYregionRdd).union(consume_regionRdd).distinct().map{line => 
      val vertexType = "region"
      val card = new MultiUtil.card_VP("")
      val region = new MultiUtil.region_VP(line.trim)
      val mchnt_cd = new MultiUtil.mchnt_cd_VP("")
      val term_id = new MultiUtil.term_id_VP("")
      val Vertex_Property = new MultiUtil.Vertex_Property_Class(vertexType,card,region,mchnt_cd,term_id)
      (HashEncode.HashMD5(line.trim), Vertex_Property)
      }
   
    val transmchntRdd = transdata.map(line => line.getString(8)) 
    val consume_mchntRdd = consumedata.map(line => line.getString(7))
    val mchntRdd = transmchntRdd.union(consume_mchntRdd).distinct().map{line => 
      val vertexType = "mchnt_cd"
      val card = new MultiUtil.card_VP("")
      val region = new MultiUtil.region_VP("")
      val mchnt_cd = new MultiUtil.mchnt_cd_VP(line.trim)
      val term_id = new MultiUtil.term_id_VP("")
      val Vertex_Property = new MultiUtil.Vertex_Property_Class(vertexType,card,region,mchnt_cd,term_id)
      (HashEncode.HashMD5(line.trim), Vertex_Property)
      }
     
   
    val QXtermRdd = quxiandata.map(line => line.getString(7)) 
    val QYtermRdd = querydata.map(line => line.getString(6)) 
    val consume_termRdd = consumedata.map(line => line.getString(9)) 
    val termRdd = QXtermRdd.union(QYtermRdd).union(consume_termRdd).distinct().map{line =>
      val vertexType = "term_id"
      val card = new MultiUtil.card_VP("")
      val region = new MultiUtil.region_VP("")
      val mchnt_cd = new MultiUtil.mchnt_cd_VP("")
      val term_id = new MultiUtil.term_id_VP(line.trim)
      val Vertex_Property = new MultiUtil.Vertex_Property_Class(vertexType,card,region,mchnt_cd,term_id)
      (HashEncode.HashMD5(line.trim), Vertex_Property)
      }
    
    val verticeRDD = cardRdd.union(regionRdd).union(mchntRdd).union(termRdd)
    
    println("verticeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )    
    

    val trans_to_RDD = transdata.map { line=>
        val srcId = HashEncode.HashMD5(line.getString(0))
        val dstId = HashEncode.HashMD5(line.getString(1))
        val money = line.getLong(2)
        val date = line.getString(3)
        val charge = line.getLong(11)
        val ForeignCount = if(line.getString(6).equals("2")) 1 else 0  // 异地交易次数+1            交易模式    1 同城   2 异地    4 跨境
        val trans_hour = line.getString(5).substring(0, 2).toInt
        val nightCount =  if(trans_hour>0 && trans_hour<6) 1 else 0  //是否夜间交易  1是 0否
        val count = 1
        
        val trans_to_EP = new MultiUtil.trans_to_EP(money,date,charge,ForeignCount, nightCount,count)
        val trans_at_EP = new MultiUtil.trans_at_EP(0L,"","","",0L,0)
        val quxian_at_EP = new MultiUtil.quxian_at_EP(0L,"","","",0L,0)
        val query_at_EP =  new MultiUtil.query_at_EP(0L,"","","",0L,0)
        val consume_at_EP = new MultiUtil.consume_at_EP(0L,"","","",0L,0) 
        val edgeType = "trans_to"
        val Edge_Property = new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
        Edge(srcId, dstId, Edge_Property)
    }
    
    
    val trans_at_RDD = transdata.map { line=>
        val cardvid = HashEncode.HashMD5(line.getString(0))
        val regionvid = HashEncode.HashMD5(line.getString(5))
        val trans_to_EP = new MultiUtil.trans_to_EP(0L,"",0,0,0,0)
        val trans_at_EP = new MultiUtil.trans_at_EP(line.getLong(2),line.getString(3),line.getString(4),line.getString(6),line.getLong(11),1)
        val quxian_at_EP = new MultiUtil.quxian_at_EP(0L,"","","",0L,0)
        val query_at_EP =  new MultiUtil.query_at_EP(0L,"","","",0L,0)
        val consume_at_EP = new MultiUtil.consume_at_EP(0L,"","","",0L,0) 
        val edgeType = "trans_at"
        val Edge_Property = new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
        Edge(cardvid, regionvid, Edge_Property)
    }
    
    val quxian_at_RDD = quxiandata.map { line=>
        val cardvid = HashEncode.HashMD5(line.getString(0))
        val regionvid = HashEncode.HashMD5(line.getString(4))
        val trans_to_EP = new MultiUtil.trans_to_EP(0L,"",0,0,0,0)
        val trans_at_EP = new MultiUtil.trans_at_EP(0L,"","","",0L,0)
        val quxian_at_EP = new MultiUtil.quxian_at_EP(line.getLong(1),line.getString(2),line.getString(3),line.getString(5),line.getLong(8),1)
        val query_at_EP =  new MultiUtil.query_at_EP(0L,"","","",0L,0)
        val consume_at_EP = new MultiUtil.consume_at_EP(0L,"","","",0L,0) 
        val edgeType = "quxian_at"
        val Edge_Property = new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
        Edge(cardvid, regionvid, Edge_Property)
    }
    
    val query_at_RDD = querydata.map { line=>
        val cardvid = HashEncode.HashMD5(line.getString(0))
        val regionvid = HashEncode.HashMD5(line.getString(3))
        val trans_to_EP = new MultiUtil.trans_to_EP(0L,"",0,0,0,0)
        val trans_at_EP = new MultiUtil.trans_at_EP(0L,"","","",0L,0)
        val quxian_at_EP = new MultiUtil.quxian_at_EP(0L,"","","",0L,0)
        val query_at_EP =  new MultiUtil.query_at_EP(0L,line.getString(1),line.getString(2),line.getString(4),line.getLong(7),1)
        val consume_at_EP = new MultiUtil.consume_at_EP(0L,"","","",0L,0) 
        val edgeType = "query_at"
        val Edge_Property = new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
        Edge(cardvid, regionvid, Edge_Property)
    }
    
       
    val consume_at_RDD = consumedata.map { line=>
        val cardvid = HashEncode.HashMD5(line.getString(0))
        val regionvid = HashEncode.HashMD5(line.getString(4))
        val trans_to_EP = new MultiUtil.trans_to_EP(0L,"",0,0,0,0)
        val trans_at_EP = new MultiUtil.trans_at_EP(0L,"","","",0L,0)
        val quxian_at_EP = new MultiUtil.quxian_at_EP(0L,"","","",0L,0)
        val query_at_EP =  new MultiUtil.query_at_EP(0L,"","","",0L,0)
        val consume_at_EP = new MultiUtil.consume_at_EP(line.getLong(1),line.getString(2),line.getString(3),line.getString(5),line.getLong(10),1)
        val edgeType = "consume_at"
        val Edge_Property = new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
        Edge(cardvid, regionvid, Edge_Property)
    }
    
 
    
    val edgeRDD = trans_to_RDD.union(trans_at_RDD).union(quxian_at_RDD).union(query_at_RDD).union(consume_at_RDD)
 
    println("edgeRDD done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
     
    transdata.unpersist(blocking=false)
    quxiandata.unpersist(blocking=false)
    querydata.unpersist(blocking=false)
    consumedata.unpersist(blocking=false)
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD).partitionBy(PartitionStrategy.RandomVertexCut)    //必须在调用groupEdges之前调用Graph.partitionBy 。
    println("graph Edge Num is: " + graph.numEdges)
    
    println("card vertices:")
    graph.vertices.filter(pred=> pred._2.vertexType.equals("card")).take(2).foreach(println)
    println("region vertices:")
    graph.vertices.filter(pred=> pred._2.vertexType.equals("region")).take(2).foreach(println)
    println("mchnt_cd vertices:")
    graph.vertices.filter(pred=> pred._2.vertexType.equals("mchnt_cd")).take(2).foreach(println)
    println("term_id vertices:")
    graph.vertices.filter(pred=> pred._2.vertexType.equals("term_id")).take(2).foreach(println)
     
    println("trans_to edges:")
    graph.edges.filter(f=>f.attr.edgeType.equals("trans_to")).take(2).foreach(println)
    println("trans_at edges:")
    graph.edges.filter(f=>f.attr.edgeType.equals("trans_at")).take(2).foreach(println)
    println("quxian_at edges:")
    graph.edges.filter(f=>f.attr.edgeType.equals("quxian_at")).take(2).foreach(println)
    println("query_at edges:")
    graph.edges.filter(f=>f.attr.edgeType.equals("query_at")).take(2).foreach(println) 
    println("consume_at edges:")
    graph.edges.filter(f=>f.attr.edgeType.equals("consume_at")).take(2).foreach(println) 
    
    
    //groupedges  只对 srcId和dstId都相相等的边进行合并， 而由于顶点是唯一的，所以srcId和dstId都相当的两条边不会出现不同类型的边，所以可以放心合并
// money,date,charge,isForeign,isnight,count
    var Ggraph = graph.groupEdges((ea,eb) => 
       ea.edgeType match {
	        case "trans_to" => {
	           val edgeType = "trans_to" 
	           val trans_to_EP = new MultiUtil.trans_to_EP(ea.trans_to_EP.money+eb.trans_to_EP.money, ea.trans_to_EP.date, ea.trans_to_EP.charge+eb.trans_to_EP.charge, 
	               ea.trans_to_EP.ForeignCount+eb.trans_to_EP.ForeignCount, ea.trans_to_EP.nightCount+eb.trans_to_EP.nightCount, ea.trans_to_EP.count+eb.trans_to_EP.count)
             val trans_at_EP = ea.trans_at_EP
             val quxian_at_EP = ea.quxian_at_EP
             val query_at_EP =  ea.query_at_EP
             val consume_at_EP = ea.consume_at_EP
             new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
	        }
	        
	        case "trans_at" => {
	           val edgeType = "trans_at" 
	           val trans_to_EP = ea.trans_to_EP
             val trans_at_EP = new MultiUtil.trans_at_EP(ea.trans_at_EP.money+eb.trans_at_EP.money, ea.trans_at_EP.date, ea.trans_at_EP.loc_trans_tm,
                 ea.trans_at_EP.trans_md, ea.trans_at_EP.total_disc_at+eb.trans_at_EP.total_disc_at, ea.trans_at_EP.count+eb.trans_at_EP.count) 
             val quxian_at_EP = ea.quxian_at_EP
             val query_at_EP =  ea.query_at_EP
             val consume_at_EP = ea.consume_at_EP
             new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
	        }
	        
	        case "quxian_at" => {
	           val edgeType = "quxian_at" 
	           val trans_to_EP = ea.trans_to_EP
	           val trans_at_EP = ea.trans_at_EP
             val quxian_at_EP = new MultiUtil.quxian_at_EP(ea.quxian_at_EP.money+eb.quxian_at_EP.money, ea.quxian_at_EP.date, ea.quxian_at_EP.loc_trans_tm,
                 ea.quxian_at_EP.trans_md, ea.quxian_at_EP.total_disc_at+eb.quxian_at_EP.total_disc_at, ea.quxian_at_EP.count+eb.quxian_at_EP.count)
             val query_at_EP =  ea.query_at_EP
             val consume_at_EP = ea.consume_at_EP
             new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
	        }
	        
	        case "query_at" => {
	           val edgeType = "query_at" 
	           val trans_to_EP = ea.trans_to_EP
	           val trans_at_EP = ea.trans_at_EP
             val quxian_at_EP = ea.quxian_at_EP
             val query_at_EP = new MultiUtil.query_at_EP(ea.query_at_EP.money+eb.query_at_EP.money, ea.query_at_EP.date, ea.query_at_EP.loc_trans_tm, 
                 ea.query_at_EP.trans_md, ea.query_at_EP.total_disc_at+eb.query_at_EP.total_disc_at, ea.query_at_EP.count+eb.query_at_EP.count)
             val consume_at_EP = ea.consume_at_EP
             new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
	        }
	        
	        case "consume_at" => {
	           val edgeType = "consume_at" 
	           val trans_to_EP = ea.trans_to_EP
	           val trans_at_EP = ea.trans_at_EP
             val quxian_at_EP = ea.quxian_at_EP
             val query_at_EP = ea.query_at_EP
             val consume_at_EP = new MultiUtil.consume_at_EP(ea.consume_at_EP.money+eb.consume_at_EP.money, ea.consume_at_EP.date, ea.consume_at_EP.loc_trans_tm,
                 ea.consume_at_EP.trans_md, ea.consume_at_EP.total_disc_at+eb.consume_at_EP.total_disc_at, ea.consume_at_EP.count+eb.consume_at_EP.count)
             new MultiUtil.Edge_Property_Class(edgeType,trans_to_EP,trans_at_EP,quxian_at_EP,query_at_EP,consume_at_EP)
	        }
	        
	        case _ => {throw new IllegalStateException("invalid StateException!")}
	      }
     )
     
     
     //Ggraph.persist(StorageLevel.MEMORY_AND_DISK_SER)
     println("Ggraph Vertex Num is: " + Ggraph.numVertices)
     println("Ggraph Edge Num is: " + Ggraph.numEdges)
    
     println("trans_to edges:")
     Ggraph.edges.filter(f=>f.attr.edgeType.equals("trans_to")).take(1).foreach(println)
     println("trans_at edges:")
     Ggraph.edges.filter(f=>f.attr.edgeType.equals("trans_at")).take(1).foreach(println)
     println("quxian_at edges:")
     Ggraph.edges.filter(f=>f.attr.edgeType.equals("quxian_at")).take(1).foreach(println)
     println("query_at edges:")
     Ggraph.edges.filter(f=>f.attr.edgeType.equals("query_at")).take(1).foreach(println) 
     println("consume_at edges:")
     Ggraph.edges.filter(f=>f.attr.edgeType.equals("consume_at")).take(1).foreach(println) 
     
     
     var cardgraph = Ggraph.subgraph(vpred = (id, attr)=>attr.vertexType.equals("card"))  //这里面可能在转账图里有很多孤立点，因为很多卡号只有取现或者消费之类的，没有转账，这些卡要去掉。
     cardgraph = cardgraph.subgraph(epred = triplet => triplet.attr.edgeType.equals("trans_to"))
     println("cardgraph Vertex Num is: " + cardgraph.numVertices)
     println("cardgraph Edge Num is: " + cardgraph.numEdges)
     
     val tempDegGraph = cardgraph.outerJoinVertices(cardgraph.degrees){
      (vid, p, DegOpt) => (p, DegOpt.getOrElse(0))
    }
    
     //去除边出入度和为2的图
    var coregraph = tempDegGraph.subgraph(epred = triplet => (triplet.srcAttr._2 + triplet.dstAttr._2) > 2)
    coregraph = coregraph.outerJoinVertices(coregraph.degrees){
       (vid, tempProperty, degOpt) => (tempProperty._1, degOpt.getOrElse(0))
    }
     
    //去除度为0的点
    val degGraph = coregraph.subgraph(vpred = (vid, property) => property._2!=0).persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    println("degGraph Vertex Num is: " + degGraph.numVertices)
    println("degGraph Edge Num is: " + degGraph.numEdges)
    
    val cgraph = ConnectedComponents.run(degGraph)
    println("create cgraph in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." ) 
    
    val connectedCount = cgraph.vertices.map(pair=>(pair._2, 1)).reduceByKey(_+_).sortBy(f => f._2, true)
    //connectedCount  (团体, 对应团体规模) 
    
    println("create connectedCount done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val ccgraph = degGraph.outerJoinVertices(cgraph.vertices){
      (vid, tempProperty, connectedOpt) => (tempProperty._1, connectedOpt)
    }
    // ccgraph   (vid,顶点属性，对应团体)
    
    val ccVertice = ccgraph.vertices.map(line => (line._1, line._2._1, line._2._2.getOrElse(0L)))
    
    println("create ccVertice done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
     
    val cCDF= connectedCount.map(p=>(p._1.toLong, p._2)).toDF("cc", "ccCount")     //原本p._1  是VertexId类型的, Row  不支持这种类型，所以需要转换成Long型
  
    val cVDF = ccVertice.map(p=>(p._1.toLong,p._2,p._3.toLong)).toDF("cardvid", "vprop", "cc")

    //cVDF (vid,顶点属性，对应团体)  join cCDF (对应团体, 团体规模)
    println("create dataframe done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    
    var joinedDF = cVDF.join(cCDF, cVDF("cc")=== cCDF("cc"), "left_outer").drop(cVDF("cc"))
    joinedDF.show(5)  //(vid,顶点属性，对应团体, 规模) 

    val VidconnectedCount = joinedDF.map(row=>(row.getLong(0), (row.getAs[MultiUtil.Vertex_Property_Class](1),row.getLong(2),row.getInt(3))))

    println("create VidconnectedCount done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    VidconnectedCount.take(5).foreach(println)
    
    //VidconnectedCount    RDD(Vid, (原顶点属性，对应团体, 对应团体规模)) 

    var cCountgraph = coregraph.outerJoinVertices(VidconnectedCount){
      (vid, oldProperty, vccCountprop) => vccCountprop.getOrElse((new  MultiUtil.Vertex_Property_Class("none",
     new MultiUtil.card_VP(""),
     new MultiUtil.region_VP(""),
     new MultiUtil.mchnt_cd_VP(""),
     new MultiUtil.term_id_VP("")), 0L, 0))}
   
//    cCountgraph = cCountgraph.subgraph(vpred = (id, attr) => !attr._1.vertexType.equals("none"))
//    cardgraph = cardgraph.subgraph(epred = triplet => triplet.attr.edgeType.equals("trans_to"))
    
    //[card,[b94ef92c38d3b3637065721165120271],[],[],[]],894527340151,3)
    
     // cCountgraph 的顶点属性为  (vid,原密卡号，对应团体, 对应团体规模)
    println("create cCountgraph done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    cCountgraph.vertices.take(1).foreach(println)
    cCountgraph.edges.take(1).foreach(println)
           
    val tempGraph1 = cCountgraph.outerJoinVertices(cardgraph.inDegrees){
      (vid, oldProperty, inDegOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, inDegOpt.getOrElse(0)) 
    }    

    
    val tempGraph2 = tempGraph1.outerJoinVertices(cardgraph.outDegrees){
      (vid, oldProperty, outDegOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, outDegOpt.getOrElse(0)) 
    }    
    
    //  (vid,原节点属性，对应团体, 对应团体规模,入度,出度)
    
    println("create tempGraph2 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    tempGraph2.vertices.take(5).foreach(println)
  
    //3.3 计算转入金额、转出金额、净转账金额
    //3.3.1 转入金额
    val sumInVDF = tempGraph2.aggregateMessages[Long](
      triplet => {
        triplet.sendToDst(triplet.attr.trans_to_EP.money)
      },
      (a, b) => (a + b)).toDF("vid","sumIn")

    //3.3.2 转出金额
    val sumOutVDF = tempGraph2.aggregateMessages[Long](
      triplet => {
        triplet.sendToSrc(triplet.attr.trans_to_EP.money)
      },
      (a, b) => (a + b)).toDF("vid","sumOut")

  
    //3.3.4  计算净金额
    var InOutDF = sumInVDF.join(sumOutVDF, sumInVDF("vid") === sumOutVDF("vid"), "left_outer").drop(sumInVDF("vid"))
    InOutDF.show(2)
 
    InOutDF = InOutDF.filter(InOutDF("sumIn").isNotNull &&  InOutDF("sumOut").isNotNull )
    val pureSum = InOutDF.map(row=>(row.getLong(1), (math.abs(row.getLong(0)-row.getLong(2)), math.abs(row.getLong(0)-row.getLong(2))/math.abs(row.getLong(0)+row.getLong(2)))))

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 

    val tempGraph3 = tempGraph2.outerJoinVertices(pureSum){
      (vid, oldProperty, pureSumOpt) => (oldProperty._1, oldProperty._2, oldProperty._3, oldProperty._4, oldProperty._5,  pureSumOpt.getOrElse((9999L,9999L))) 
    }    
    
    //  (vid,原属性，对应团体, 对应团体规模 ,入度,出度, 净流通金额)
    
    println("create tempGraph3 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    
    //统计每个连通图顶点属性
    val ccVerticeRdd = tempGraph3.vertices.map{f =>
      val ccLabel = f._2._2   //团体号
      val vid = f._1  //顶点号,暂时不用
      val vprop = f._2._1  //顶点属性  ,暂时不用
      val ccCount = f._2._3  //团体规模
      val inDeg = f._2._4
      val outDeg = f._2._5
      val degree = inDeg + outDeg
      val transNode = if(f._2._6._1.get < 500 || f._2._6._2.get < 0.01 ) 1 else 0 
      (ccLabel, (ccCount, inDeg, outDeg, degree, transNode))
    }.reduceByKey((r1, r2) => (r1._1, math.max(r1._2, r2._2), math.max(r1._3, r2._3), math.max(r1._4, r2._4), r1._5+r2._5))
    
   //统计每个连通图边的属性  
    var ccEdgeGroupRdd = tempGraph3.triplets.map(f => {
      val ccLabel = f.srcAttr._2   //团体号 
      val transCount = f.attr.trans_to_EP.count
      val money = f.attr.trans_to_EP.money
      val foreignCount = f.attr.trans_to_EP.ForeignCount
      val nightCount = f.attr.trans_to_EP.nightCount
      val charge = f.attr.trans_to_EP.charge
      (ccLabel, (money, transCount, foreignCount, nightCount,charge))
    }).reduceByKey((e1, e2) => (e1._1+e2._1, e1._2+e2._2, e1._3+e2._3, e1._4+e2._4, e1._5+e2._5))
 
    println("ccgraphRdd created in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    ccEdgeGroupRdd.take(5).foreach(println)
    
//////////////////////trans地点统计/////////////////////////////////
    var trans_at_graph = Ggraph.subgraph(epred = triplet => triplet.attr.edgeType.equals("trans_at"))
    println("trans_at_graph Vertex Num is: " + trans_at_graph.numVertices)
    println("trans_at_graph Edge Num is: " + trans_at_graph.numEdges)
    
    val trans_at_DF = trans_at_graph.edges.map(v => (v.srcId, v.dstId)).toDF("cardvid", "regionvid")
    trans_at_graph.unpersist(blocking=false)
    
    var cc_region_DF = cVDF.join(trans_at_DF, cVDF("cardvid")=== trans_at_DF("cardvid"), "left_outer").drop(cVDF("cardvid"))
    //(cc,regionvid)
    
    println("All flow done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val cc_regions = cc_region_DF
      .select("cc", "regionvid")
      .groupBy('cc)
      .agg('cc, countDistinct('regionvid) as "count_region")    //增加了一列 "count_region"
      .map(f => {
        (f.getLong(0), f.getLong(2))   //"cc", "regionvid", "count_region"  选"cc", "count_region"
      })
      
    cc_regions.take(5).foreach(println)
    
    
    val region_ccs = cc_region_DF
      .select("regionvid", "cc")
      .groupBy('regionvid)
      .agg('regionvid, countDistinct('cc) as "count_cc")    //增加了一列 "count_region"
      .map(f => {
        (f.getLong(0), f.getLong(2))   
      })
    
    region_ccs.take(5).foreach(println) 
////////////////////////////////////////////////////////////////////
    
    
    
    
    
    
    sc.stop()
    
  } 
  
} 
