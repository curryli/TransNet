package AlgorithmUtil

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
 

/**
 * spark-submit  \
 * --master yarn \
 * --queue root.spark \
 * --num-executors  100 \
 * --executor-memory 4G \
 * --class com.arlab.graphx.test.TestV2 \
 * /home/hdanaly/arlab/jar/GraphXTest.jar
 *
 */
object TestV4 {
  class VertexProperty()
  class EdgePropery()

  //顶点: 卡片，属性：id, 帐号，
  case class CardVertex(val priAcctNo: String, val inDgr: Int, val outDgr: Int, val graphLabel: Long, val graphSize: Int, val sumIn: Int, val sumOut: Int, val income: Int) extends VertexProperty

  case class TransferProperty(val transAt: Int, val count: Int, val transDt: String, val region_cd: String, val mchntTp: String, val mchntCd: String, val cardAccptrNmAddr: String) extends EdgePropery

  // The graph might then have the type:
  var graph: Graph[VertexProperty, EdgePropery] = null

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.WARN);

    require(args.length == 1)
    val date = args(0)

    val conf = new SparkConf().setAppName("Graphx Test")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    //    var data = hc.sql(s"select tfr_in_acct_no,tfr_out_acct_no,default.md5_int(tfr_in_acct_no) as tfr_in_acct_no_id, default.md5_int(tfr_out_acct_no) as tfr_out_acct_no_id,trans_at  from tbl_common_his_trans where " +
    //      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') > unix_timestamp('20161001','yyyyMMdd')  and " +
    //      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('20160601','yyyyMMdd')  and " +
    //      s"trans_id='S33'")

    val data = hc.sql(s"select tfr_in_acct_no,cast (md5_int(tfr_in_acct_no) as bigint) as tfr_in_acct_no_id," +
      s"tfr_out_acct_no,cast (md5_int(tfr_out_acct_no) as bigint) as tfr_out_acct_no_id," +
      s"cast (trans_at as int) ,to_ts, region(substring(acpt_ins_id_cd,-4,4)) as region_cd,mchnt_tp,mchnt_cd,card_accptr_nm_addr " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt=$date    and " +
      s"sti_takeout_in='1'  and cu_trans_st='10000' and resp_cd1='00'  and " +
      s"trans_id !='S33' and tfr_in_acct_no!='' and tfr_out_acct_no!=''  ").repartition(100)

    println("start sql ....")

    //1.构建边:转出卡、转入卡、转账金额、转账时间
    val transferRelationRDD = data.map {
      r =>
        val item = new TransferProperty(r.getInt(4), 1, r.getString(5), r.getString(6), r.getString(7), r.getString(8), r.getString(9)) //金额、次数，时间,地区，商户类型，商户代码，地址
        Edge(r.getLong(3), r.getLong(1), item) // srcId,destId
    }

    //2.构建顶点：

    //2.1 转入卡
    val inCardRDD = data.map {
      r =>
        val item = new CardVertex(r.getString(0), 0, 0, 0, 0, 0, 0, 0)
        (r.getLong(1), item) //destId
    }

    //2.2 转出卡
    val outCardRDD = data.map {
      r =>
        val item = new CardVertex(r.getString(2), 0, 0, 0, 0, 0, 0, 0)
        (r.getLong(3), item) //srcId
    }

    //2.3 顶点合并
    val cardRDD = inCardRDD.union(outCardRDD).distinct()

    //3. 构建图
    var g = Graph(cardRDD, transferRelationRDD).partitionBy(PartitionStrategy.RandomVertexCut)

    //3.1  边聚合
        g = g.groupEdges((a, b) => new TransferProperty(a.transAt + b.transAt, a.count + b.count, a.transDt.substring(0, 10),a.region_cd,a.mchntTp,a.mchntCd,a.cardAccptrNmAddr))

    //3.2 计算出入度
    g = g.outerJoinVertices(g.inDegrees) {
      (id, v, inDegOpt) => CardVertex(v.priAcctNo, inDegOpt.getOrElse(0), v.outDgr, 0, 0, 0, 0, 0)
    }.outerJoinVertices(g.outDegrees) {
      (id, v, outDegOpt) => CardVertex(v.priAcctNo, v.inDgr, outDegOpt.getOrElse(0), 0, 0, 0, 0, 0)
    }
    //3.3 计算转入金额、转出金额、净转账金额
    //3.3.1 转入金额
    val sumInVRdd = g.aggregateMessages[Int](
      triplet => {
        triplet.sendToDst(triplet.attr.transAt)
      },
      (a, b) => (a + b))

    //3.3.2 转出金额
    val sumOutVRdd = g.aggregateMessages[Int](
      triplet => {
        triplet.sendToSrc(triplet.attr.transAt)
      },
      (a, b) => (a + b))

    //3.3.3 转入、转出金额添加到图中
    g = g.outerJoinVertices(sumInVRdd) {
      (id, v, sumIn) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, 0, 0, sumIn.getOrElse(0), 0, 0)
    }.outerJoinVertices(sumOutVRdd) {
      (id, v, sumOut) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, 0, 0, v.sumIn, -sumOut.getOrElse(0), 0)
    }

    //3.3.4  计算净金额
    val incomeVRdd = g.vertices.mapValues((vid, v) => (v.sumIn + v.sumOut))
    g = g.outerJoinVertices(incomeVRdd) {
      (id, v, u) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, 0, 0, v.sumIn, v.sumOut, u.getOrElse(0))
    }

    //3.5 筛选边出入度和为2的图:两张卡号单向转账>=1次,待用
    val degree2Graph = g.subgraph(vpred => (vpred.srcAttr.inDgr + vpred.srcAttr.outDgr + vpred.dstAttr.inDgr + vpred.dstAttr.outDgr) == 2)

    //3.6 去除边出入度和为2的图
    var gV1 = g.subgraph(vpred => (vpred.srcAttr.inDgr + vpred.srcAttr.outDgr + vpred.dstAttr.inDgr + vpred.dstAttr.outDgr) > 2)

    //重新添加出入度，为了去掉degree2Graph的顶点
    gV1 = gV1.outerJoinVertices(gV1.inDegrees) {
      (id, v, inDegOpt) => CardVertex(v.priAcctNo, inDegOpt.getOrElse(0), v.outDgr, 0, 0, v.sumIn, v.sumOut, v.income)
    }.outerJoinVertices(gV1.outDegrees) {
      (id, v, outDegOpt) => CardVertex(v.priAcctNo, v.inDgr, outDegOpt.getOrElse(0), 0, 0, v.sumIn, v.sumOut, v.income)
    }

    //根据顶点的出入度，筛选顶点:边和点剩余15%
    var gV2 = gV1.subgraph(vpred = (id, card) => !(card.inDgr == 0 && card.outDgr == 0))

    val gV3 = ConnectedComponents.run(gV2)
    val graphCount = gV3.vertices.map(f => (f._2, 1)).reduceByKey(_ + _).sortBy(f => f._2, false, 1) //标签计数
    println(graphCount.collect().mkString("\n"))
    println(graphCount.count)

    // joinVertices 与outerJoinVertices 区别：

    // joinVertices未匹配到的用原值
    // outerJoinVertices，未匹配到的赋值0
    gV2 = gV2.joinVertices(gV3.vertices)((vid, v, u) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, u, 0, v.sumIn, v.sumOut, v.income)) //顶点加子图标签:联通图的最小id
    gV2 = gV2.joinVertices(graphCount)((vid, v, u) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, v.graphLabel, u, v.sumIn, v.sumOut, v.income)) //顶点加子图标签：连通子图规模
    val edgesCount=gV2.edges.count()
    val diffCount=gV2.triplets.filter(f=>f.dstAttr.graphLabel.equals(f.srcAttr.graphLabel)).count()
    println("gv2 edges count:\t"+edgesCount+"vertice lable same count:"+diffCount)
    
    
    //    gV2.triplets.filter(f=>f.dstAttr.graphLabel=="1623409183641"||f.srcAttr.graphLabel=="1623409183641").count
    //    gV2.vertices.filter(f=>f._2.graphLabel=="1623409183641").count

    var label = 7591201976672L
     graphCount.filter(f => f._2 >=100).collect().foreach(f=>{
       
          label = f._1
          println(f._1+"\t"+f._2)
          var sampleG = gV2.subgraph(vpred = (id, u) => u.graphLabel.equals(label),
            epred = epred => epred.srcAttr.graphLabel.equals(label) ||
              epred.dstAttr.graphLabel.equals(label))

          sampleG.edges.saveAsTextFile("/user/hdanaly/arlab/data/"+date+"/edges_" +f._2+"_"+ label)
          sampleG.vertices.saveAsTextFile("/user/hdanaly/arlab/data/"+date+"/vertices_" +f._2+"_" + label)
          sampleG.unpersist(blocking = false)
    
       
     })
    
//         graphCount.filter(f => f._2 > 10).foreach(f=>{
//       
//          label = f._1
//          println(f._1+"\t"+f._2)
//  
////          var sampleG = gV2.subgraph(vpred = (id, u) => u.graphLabel.equals(label),
////            epred = epred => epred.srcAttr.graphLabel.equals(label) ||
////              epred.dstAttr.graphLabel.equals(label))
////
////          sampleG.edges.coalesce(1,true).saveAsTextFile("/user/hdanaly/arlab/data/"+date+"/edges_" +f._2+"_"+ label)
////          sampleG.vertices.coalesce(1,true).saveAsTextFile("/user/hdanaly/arlab/data/"+date+"/vertices_" +f._2+"_" + label)
////          sampleG.unpersist(blocking = false)
//       
//     })
//    


    //   var sampleG= gV2.subgraph(vpred=(id,u)=>u.graphLabel.equals(label),
    //                             epred=epred=>epred.srcAttr.graphLabel.equals(label) ||
    //                                     epred.dstAttr.graphLabel.equals(label))
    //   println("sampleG edges count:\t"+sampleG.edges.count())
    //   println("sampleG vertices count:\t"+sampleG.vertices.count())

    //   sampleG.edges.saveAsTextFile("/user/hdanaly/arlab/data/edges_0718"+label)
    //   sampleG.vertices.saveAsTextFile("/user/hdanaly/arlab/data/vertices_0718"+label)
    //   
    //    gV2.edges.saveAsTextFile("/user/hdanaly/arlab/data/edges_1118")
    //    gV2.vertices.saveAsTextFile("/user/hdanaly/arlab/data/vertices_1118")

    //    graphCount.saveAsTextFile("/user/hdanaly/arlab/data/count_20161218")

  }

}