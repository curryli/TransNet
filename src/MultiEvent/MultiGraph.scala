package MultiEvent

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable 
import scala.io.Source  
import org.apache.spark.storage.StorageLevel
import AlgorithmUtil._

object MultiGraph {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.INFO);
    Logger.getLogger("akka").setLevel(Level.INFO);
    Logger.getLogger("hive").setLevel(Level.INFO);
    Logger.getLogger("parse").setLevel(Level.INFO);
 
    //设置运行环境
    val conf = new SparkConf().setAppName("MultiGraph") 
    val sc = new SparkContext(conf)
     
    val InOutfile = sc.textFile("xrli/MultiEvent/staticInOut4.txt").persist(StorageLevel.MEMORY_AND_DISK_SER)  
    val InPairRdd = InOutfile.map(line => line.split("\\s+")(0))                        
    val OutPairRdd = InOutfile.map(line => line.split("\\s+")(1))      
    
    val cardRdd = InPairRdd.union(OutPairRdd).distinct().map{line => 
      val card = line.trim
      val location = ""
      (HashEncode.HashMD5(card), (card, location))
      }
    
    val Locationfile = sc.textFile("xrli/MultiEvent/disLocMap.txt")
    val LocationRdd = Locationfile.map{line => 
      val card = ""
      val location = line.trim
      (HashEncode.HashMD5(location),(card, location)) 
    }
    
    val verticeRDD = cardRdd.union(LocationRdd)

  
    val transRDD = InOutfile.map { line=>
        val lineArray = line.split("\\s+")
        val srcId = HashEncode.HashMD5(lineArray(0))
        val dstId = HashEncode.HashMD5(lineArray(1))
        val transAmount = lineArray(2).toDouble
        val quxianAmount = 0.0
        val quxianCount = 0
        val queryCount = 0
        val edgeType = "trans_to"
        Edge(srcId, dstId, (edgeType,transAmount,quxianCount,queryCount))
    }
    
    InOutfile.unpersist(blocking=false)
    
    val quxianfile = sc.textFile("xrli/MultiEvent/Quxian_Map4.txt")
    val quxianRDD = quxianfile.map { line=>
        val lineArray = line.split("\\s+")
        val srcId = HashEncode.HashMD5(lineArray(0))
        val dstId = HashEncode.HashMD5(lineArray(1))
        val transAmount = 0.0
        val quxianAmount = lineArray(2).toDouble
        val quxianCount = lineArray(3).toInt
        val queryCount = 0
        val edgeType = "quxian_at"
        Edge(srcId, dstId, (edgeType,transAmount,quxianCount,queryCount))
    }
    
    
    val queryfile = sc.textFile("xrli/MultiEvent/Query_Mao4.txt")
    val queryRDD = queryfile.map { line=>
        val lineArray = line.split("\\s+")
        val srcId = HashEncode.HashMD5(lineArray(0))
        val dstId = HashEncode.HashMD5(lineArray(1))
        val transAmount = 0.0
        val quxianAmount = 0.0
        val quxianCount = 0
        val queryCount = lineArray(2).toInt
        val edgeType = "query_at"
        Edge(srcId, dstId, (edgeType,transAmount,quxianCount,queryCount))
    }
    
    val edgeRDD = transRDD.union(quxianRDD).union(queryRDD)
 
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD) 
    println("card vertices:")
    graph.vertices.filter(pred=> !pred._2._1.isEmpty()).take(2).foreach(println)
    println("location vertices:")
    graph.vertices.filter(pred=> !pred._2._2.isEmpty()).take(2).foreach(println)
    
    println("trans edges:")
    graph.edges.filter(f=>f.attr._1.equals("trans_to")).take(2).foreach(println)
    println("quxian edges:")
    graph.edges.filter(f=>f.attr._1.equals("quxian_at")).take(2).foreach(println)
    println("query edges:")
    graph.edges.filter(f=>f.attr._1.equals("query_at")).take(2).foreach(println)

   
    sc.stop()
  }
}





//card vertices:
//(73611358943974,(2769,))
//(821496748247487,(3828,))

//location vertices:
//(260181179446888,(,Loc6739))
//(232085492525538,(,Loc1000))

//trans edges:
//Edge(1867493586470,616201402138654,(trans_to,42000.0,0,0))
//Edge(2240198402025,85755944688691,(trans_to,1800.0,0,0))

//quxian edges:
//Edge(684975952044,491098563329389,(quxian_at,0.0,1,0))
//Edge(1584049003518,491098563329389,(quxian_at,0.0,15,0))

//query edges:
//Edge(320650467136,623270938700847,(query_at,0.0,0,12))
//Edge(684975952044,623270938700847,(query_at,0.0,0,4))