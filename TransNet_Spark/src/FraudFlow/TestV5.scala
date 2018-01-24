package FraudFlow
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import AlgorithmUtil._

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
object TestV5 {
  /**
   * 过渡节点限额：500分
   */
  val transAtFlag = 500

  class VertexProperty()
  class EdgePropery()

  //顶点: 卡片，属性：id, 帐号，
  case class CardVertex(
    val priAcctNo: String,
    val inDgr: Int,
    val outDgr: Int,
    val graphLabel: Long,
    val graphSize: Int,
    val sumIn: Int,
    val sumOut: Int,
    val income: Int) extends VertexProperty

  case class TransferProperty(
    val transAt: Int,
    val count: Int,
    val transDt: String,
    val region_cd: String,
    val mchntTp: String,
    val mchntCd: String,
    val cardAccptrNmAddr: String,
    val totalDiscAt: Int,
    val graphLabel: Long) extends EdgePropery

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

    val data = hc.sql(s"select " +
      s"tfr_in_acct_no,cast (md5_int(tfr_in_acct_no) as bigint) as tfr_in_acct_no_id," +
      s"tfr_out_acct_no,cast (md5_int(tfr_out_acct_no) as bigint) as tfr_out_acct_no_id," +
      s"cast (trans_at as int) ,to_ts, region(substring(acpt_ins_id_cd,-4,4)) as region_cd,mchnt_tp,mchnt_cd,card_accptr_nm_addr ," +
      s"cast(total_disc_at as int) " +
      s"from hbkdb.dtdtrs_dlt_cups where " +
      s"hp_settle_dt=$date    and " +
      s"sti_takeout_in='1'  and cu_trans_st='10000' and resp_cd1='00'  and " +
      s"trans_id !='S33' and tfr_in_acct_no!='' and tfr_out_acct_no!='' ").repartition(100)

    println("start sql ....")

    //1.构建边:转出卡、转入卡、转账金额、转账时间
    val transferRelationRDD = data.map {
      r =>
        val item = new TransferProperty(
          r.getInt(4), //金额
          1, //次数
          r.getString(5), //时间
          r.getString(6), //地区
          r.getString(7), //商户类型
          r.getString(8), //商户代码
          r.getString(9), //地址
          r.getInt(10), //手续费
          0) //图标签
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
    g = g.groupEdges((a, b) => new TransferProperty(
      a.transAt + b.transAt,
      a.count + b.count,
      a.transDt.substring(0, 10),
      a.region_cd,  //转账地址 到省市
      a.mchntTp,
      a.mchntCd,
      a.cardAccptrNmAddr, //更具体的转账地址
      a.totalDiscAt + b.totalDiscAt,   
      a.graphLabel))

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
    var degreeG = gV1.subgraph(vpred = (id, card) => !(card.inDgr == 0 && card.outDgr == 0))

    val ccGraph = ConnectedComponents.run(degreeG)
    val graphCount = ccGraph.vertices.map(f => (f._2, 1)).reduceByKey(_ + _).sortBy(f => f._2, false, 1) //标签计数
    val ccGraphVertices = ccGraph.vertices.map(f => (f._1, f._2))

    import sqlContext.implicits._
    val graphCountDF = graphCount.toDF("clabel", "count")
    val ccGraphVerticesDF = ccGraphVertices.toDF("id", "label")
    val joinGraphCountDF = ccGraphVerticesDF.join(graphCountDF, ccGraphVerticesDF("label") === graphCountDF("clabel"), "left_outer").drop("label")

    val joinGraphCountRdd = joinGraphCountDF.map { f => (f.getLong(0), (f.getLong(1), f.getInt(2))) }

    //    println(graphCount.collect().mkString("\n"))
    println(graphCount.count)
    //    val ccGraphVertices=ccGraph.vertices.leftJoin(other)(f)

    // joinVertices 与outerJoinVertices 区别：

    // joinVertices未匹配到的用原值
    // outerJoinVertices，未匹配到的赋值0
    degreeG = degreeG.joinVertices(ccGraph.vertices)((vid, v, u) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, u, 0, v.sumIn, v.sumOut, v.income)) //顶点加子图标签:联通图的最小id
    degreeG = degreeG.joinVertices(joinGraphCountRdd)((vid, v, u) => CardVertex(v.priAcctNo, v.inDgr, v.outDgr, v.graphLabel, u._2, v.sumIn, v.sumOut, v.income)) //顶点加子图标签：连通子图规模

    val edgesCount = degreeG.edges.count()
    val diffCount = degreeG.triplets.filter(f => f.dstAttr.graphLabel.equals(f.srcAttr.graphLabel)).count()
    println("degreeG edges count:\t" + edgesCount + "\nvertice lable same count:" + diffCount)

    val label = 18207015455433l
    var sampleG = degreeG.subgraph(vpred = (id, u) => u.graphLabel.equals(label),
      epred = epred => epred.srcAttr.graphLabel.equals(label) ||
        epred.dstAttr.graphLabel.equals(label))

    sampleG.edges.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/edges_" + "_" + label)
    sampleG.vertices.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/vertices_" + "_" + label)

    //graphVRdd"(label,(vCount,transitionVFlag,inDegree,outDegree,income))    
    val graphVRdd = degreeG.vertices.map(f => {
      val label = f._2.graphLabel
      val vCount = f._2.graphSize
      var transitionVFlag = 0
      if (f._2.income.abs <= transAtFlag)
        transitionVFlag = 1
      val inDegree = f._2.inDgr
      val outDegree = f._2.outDgr
      val income = f._2.income
      (label, (vCount, transitionVFlag, inDegree, outDegree, income))
    })
      .reduceByKey((r1, r2) => (r1._1, r1._2 + r2._2, math.max(r1._3, r2._3), math.max(r1._4, r2._4), r1._5 + r2._5))

    var graphERdd = degreeG.triplets.map(f => {
      (f.srcAttr.graphLabel, (f.attr.count, f.attr.transAt, f.attr.totalDiscAt))
    }).reduceByKey((e1, e2) => (e1._1 + e2._1, e1._2 + e2._2, e1._3 + e2._3))

    val graphEMchntDF = degreeG.triplets.map { f =>
      {
        (f.srcAttr.graphLabel, f.attr.region_cd, f.attr.mchntTp, f.attr.mchntCd, f.attr.cardAccptrNmAddr)
      }
    }.toDF("label", "region_cd", "mchnt_tp", "mchnt_cd", "card_accptr_nm_addr")

    val regionCdRdd = graphEMchntDF
      .select("label", "region_cd")
      .groupBy('label)
      .agg('label, countDistinct('region_cd))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })
    val mchntTpRdd = graphEMchntDF
      .select("label", "mchnt_tp")
      .groupBy('label)
      .agg('label, countDistinct('mchnt_tp))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })
    val mchntCdRdd = graphEMchntDF
      .select("label", "mchnt_cd")
      .groupBy('label)
      .agg('label, countDistinct('mchnt_cd))
      .map(f => {
        (f.getLong(0), f.getLong(2))
      })

    //    val cardAccptrNmAddrRdd= graphEMchntRdd
    //                    .select("label", "card_accptr_nm_addr")
    //                    .groupBy('label)
    //                    .agg('label, countDistinct('card_accptr_nm_addr))
    //                    .map(f=>{
    //                      (f.getLong(0),f.getInt(1))
    //                    })
    //                     

    // graphERddV2：(label,(eCount,transAt,totalDiscAt,regionCdCount,mchntTpCount,mchntCdCount)
    val graphERddV2 = graphERdd.leftOuterJoin(regionCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(0)))
    }).leftOuterJoin(mchntTpRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2.getOrElse(0)))
    }).leftOuterJoin(mchntCdRdd).map(f => {
      (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._2.getOrElse(0)))
    })

    //graphVRdd"(label,(vCount,transitionVFlag,inDegree,outDegree,income))    
    val graphRdd = graphVRdd.leftOuterJoin(graphERddV2).map(f => {
      (f._1, f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5,
        f._2._2.get._1, f._2._2.get._2, f._2._2.get._3, f._2._2.get._4, f._2._2.get._5, f._2._2.get._5)
    })

    graphVRdd.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/graphVRdd")
    graphERddV2.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/graphERdd")
    graphRdd.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/graph")
    degreeG.vertices.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/vertices")
    degreeG.edges.saveAsTextFile("/user/hdanaly/arlab/data/" + date + "/edges")

  }

}