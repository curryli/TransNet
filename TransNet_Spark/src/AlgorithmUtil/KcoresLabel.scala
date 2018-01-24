package AlgorithmUtil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._
import scala.reflect.ClassTag
import SparkContext._ 
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object KcoresLabel {
  
  def maxKVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], KNum:Int) = {

    var degreeGraph = graph.outerJoinVertices(graph.degrees) {
     (vid, oldData, newData) => newData.getOrElse(0)
    }.cache()  
     
    var lastVerticeNum: Long = degreeGraph.numVertices 
    var thisVerticeNum: Long = -1
    var isConverged = false
    val maxIter = 1000
    var i = 1
    
    while (!isConverged && i <= maxIter) {  //删除 degre e< kNum 的顶点
    var subGraph = degreeGraph.subgraph(
       vpred = (vid, degree) => degree >= KNum
    ).cache()

    
    //subGraph = subGraph.subgraph(epred = triplet => triplet.srcAttr!=0 && triplet.dstAttr!= 0)
    
    degreeGraph = subGraph.outerJoinVertices(subGraph.degrees) {   //重新生成新的degreeGraph
     (vid, vd, degree) => degree.getOrElse(0)
    }.cache()

    thisVerticeNum = degreeGraph.numVertices
    
    if (lastVerticeNum == thisVerticeNum) 
      isConverged = true
    else
      lastVerticeNum = thisVerticeNum

    i += 1
   } 
    
    degreeGraph.vertices.filter(pred=> pred._2>0)

  }
  
  

  def KLabeledVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], KNum:Int, sc: SparkContext): RDD[(Long, Int)] = {
    val sqlContext = new SQLContext(sc)  
    import sqlContext.implicits._   
    var tempGraph = graph.outerJoinVertices(graph.degrees) {
         (vid, oldData, newData) => newData.get
        }.cache()
        
    var computeGraph = tempGraph
    computeGraph.cache
    
    tempGraph.unpersist(blocking = false)
    

    var allVertices = computeGraph.vertices.mapValues(a => 0)   //所有顶点maxK属性初始化为0

    val schema = StructType(StructField("vid",LongType,true)::StructField("maxK",IntegerType,true):: Nil)
    var rowRDDAll = allVertices.map(p=>Row(p._1.toLong, p._2))     //原本p._1  是VertexId类型的, Row  不支持这种类型，所以需要转换成Long型
    
    var curVdd: RDD[(Long, Int)] = null  // sc.makeRDD(Array((0L,0)))
    
    var k=1
    var noSubGraph = false
    
    var PreG: Graph[Int, ED] = null
    while(k< KNum && !noSubGraph){
           var allVerticesDF = sqlContext.createDataFrame(rowRDDAll, schema)
          
           var  graphSave = computeGraph.subgraph(
              vpred = (vid, degree) => degree == k,
              epred = triplet => triplet.srcAttr!=0 && triplet.dstAttr!= 0
              ).cache() 
            
           PreG = graphSave 
            graphSave = graphSave.outerJoinVertices(graphSave.degrees) {
               (vid, oldData, newData) => k
              }
            graphSave.cache()
            //PreG.unpersist(blocking = false)          http://f.dataguru.cn/thread-735784-1-1.html
            PreG.unpersistVertices(blocking = false)  
            PreG.edges.unpersist(blocking = false)
            
            var saveVertices = graphSave.vertices

              var lastVerticeNum: Long = computeGraph.numVertices 
              var thisVerticeNum: Long = -1
              var isConverged = false
              val maxIter = 1000
              var i = 1
          
              
              var VerticesSaveRDD = curVdd
              
              
          var Pregraph: Graph[Int, ED] = null 
          
          while (!isConverged && i <= maxIter) {  //删除 degre e< kNum 的顶点
              var subGraph = computeGraph.subgraph(
              vpred = (vid, degree) => degree > k,
              epred = triplet => triplet.srcAttr!=0 && triplet.dstAttr!= 0
              )

              if(subGraph.numVertices==0)
                noSubGraph = true
                 
              computeGraph = subGraph.outerJoinVertices(subGraph.degrees) {   //重新生成新的degreeGraph
                 (vid, vd, degree) => degree.getOrElse(0)
                }
              
              
              Pregraph = computeGraph
              computeGraph = computeGraph.subgraph(
              vpred = (vid, degree) => degree > k,
              epred = triplet => triplet.srcAttr!=0 && triplet.dstAttr!= 0
              )
               
              computeGraph.cache()
              Pregraph.unpersist(blocking = false)
                        
                        
              val kv = computeGraph.degrees.filter(f=>f._2==k)
              
            
              VerticesSaveRDD = saveVertices.union(kv)
              
              
              thisVerticeNum = computeGraph.numVertices
                
                if (lastVerticeNum == thisVerticeNum) 
                  isConverged = true
                else
                  lastVerticeNum = thisVerticeNum
              
                i += 1
             } 
          
              val rowRDDsave = VerticesSaveRDD.map(p=>Row(p._1.toLong, p._2))     //原本p._1  是VertexId类型的, Row  不支持这种类型，所以需要转换成Long型
              val saveVerticesDF = sqlContext.createDataFrame(rowRDDsave, schema)
      
              var tempDF = allVerticesDF.join(saveVerticesDF, allVerticesDF("vid") === saveVerticesDF("vid"), "left_outer").drop(saveVerticesDF("vid")) 
              tempDF.show()  //这里没有show结果就不对，暂时不知道为什么
              
              curVdd = tempDF.map{ x =>
               (x.getLong(0), 
                              if(!x.isNullAt(2))
                                 x.getInt(2)  
                              else  
                                 x.getInt(1) 
                ) }
                      
              rowRDDAll = curVdd.map(p=>Row(p._1.toLong, p._2)) 
               
              //computeGraph = computeGraph
              
              k = k+1
        }

        println("lastK is " + k)

        curVdd.map{ x =>(x._1,  if (x._2 == 0) (k-1)  else x._2)}.filter(f=>f._1!=0L) 
          
  }

}