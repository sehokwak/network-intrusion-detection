package egonetFeatures

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object featureExtraction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Feature Extraction!")
      //.master("local[*]") // if running locally
      //.master("spark://your.ip.address:7077") // if running on a standalone cluster
      //.config("spark.jars", "path/to/jar/networkintrusion_2.12-0.1.jar") // may be needed for running on cluster
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    type VertexAttribute = (Double, String, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)

    // load vertices from preprocessed vertex file
    val newVertices: RDD[(VertexId, VertexAttribute)] = sc.textFile(args(0)).map {
      line =>
        val row = line.split(",")
        (row(0).toInt, (row(1).toDouble, row(2), row(3), row(4).toDouble, row(5).toDouble,
          row(6).toDouble, row(7).toDouble, row(8).toDouble, row(9).toDouble, row(10).toDouble,
          row(11).toDouble, row(12).toDouble, row(13).toDouble, row(14).toDouble, row(15).toDouble,
          row(16).toDouble, row(17).toDouble, row(18).toDouble))
    }
    // load vertex attributes from preprocessed vertex file
    val vertLabels: RDD[(VertexId, String)] = sc.textFile(args(0)).map {
      line =>
        val row = line.split(",")
        (row(0).toInt, row(19))
    }

    // Get the max values
    val maxFirst: (VertexId, VertexAttribute) = newVertices.reduce(
      (a, b) => if (a._2._1 > b._2._1) a else b
    )
    val maxFourth: (VertexId, VertexAttribute) = newVertices.reduce(
      (a, b) => if (a._2._4 > b._2._4) a else b
    )
    val maxFifth: (VertexId, VertexAttribute) = newVertices.reduce(
      (a, b) => if (a._2._5 > b._2._5) a else b
    )
    val maxValues: (Double, Double, Double) = (maxFirst._2._1, maxFourth._2._4, maxFifth._2._5)


    // calculates the distance (ie edge weight) between two vertices
    def pow2(x: Double) = x * x

    def getDistance(t1: VertexAttribute, t2: VertexAttribute): Double = {
      val n1 = (t1._1 / maxValues._1, t1._4 / maxValues._2, t1._5 / maxValues._3)
      val n2 = (t2._1 / maxValues._1, t2._4 / maxValues._2, t2._5 / maxValues._3)
      val s1 = if (t1._2 == t2._2) 0 else 1 // for string attributes give them a 1 if equal, else 0
      val s2 = if (t1._3 == t2._3) 0 else 1
      val wgt = pow2(n1._1 - n2._1) + s1 + s2 + pow2(n1._2 - n2._2) + pow2(n1._3 - n2._3) +
        pow2(t1._6 - t2._6) + pow2(t1._7 - t2._7) + pow2(t1._8 - t2._8) + pow2(t1._9 - t2._9) +
        pow2(t1._10 - t2._10) + pow2(t1._11 - t2._11) + pow2(t1._12 - t2._12) + pow2(t1._13 - t2._13) +
        pow2(t1._14 - t2._14) + pow2(t1._15 - t2._15) + pow2(t1._16 - t2._16) + pow2(t1._17 - t2._17) +
        pow2(t1._18 - t2._18) // not sqrt here because we square the distance
      1 / math.exp(wgt)
    }

    // returns an EdgeRDD for edges in the kNN graph
    def getkNNEdges(v: RDD[(VertexId, VertexAttribute)], k: Int): RDD[Edge[Double]] = {
      val v2 = v.collect
      val edges = v.map { case (id1, t1) => (id1, v2.map { case (id2, t2) => (id2, getDistance(t1, t2)) }
        .sortWith((v1, v2) => v1._2 > v2._2).slice(2, k + 2))
      }
        .flatMap(a => a._2.map(b => Edge(a._1, b._1, b._2)))
      edges
    }

    // build the kNN graph
    val knnEdges = getkNNEdges(newVertices, k = args(2).toInt)
    val knnGraph = Graph(vertLabels, knnEdges).cache()

    // get the egonets
    val listOfEgonets: VertexRDD[Array[VertexId]] = knnGraph.aggregateMessages[Array[VertexId]](
      triplet => triplet.sendToSrc(Array(triplet.dstId)), _ ++ _
    )

    // graph with egonets as vertex property
    val egoGraph = Graph(listOfEgonets, knnEdges).cache()



    // ===== Graph Measures =====

    // PAGERANK
    val prgraph = knnGraph.pageRank(0.0001).cache()
    // pRanks of outgoing vertices
    val pRanks = prgraph.aggregateMessages[Double](
      triplet => {
        triplet.sendToSrc(triplet.dstAttr)
      },
      _ + _
    )
    // pRanks of each egonet
    val pRankEgonet = prgraph.vertices.join(pRanks).map {
      case (id, (rank1, rank2)) => (id, rank1 + rank2)
    }.cache()


    // EDGE COUNT
    // outdegree of each vertex
    val outDeg: VertexRDD[Int] = egoGraph.outDegrees

    // get number of edges in egonet not stemming from ego
    val ec: RDD[(VertexId, Int)] = egoGraph.aggregateMessages[Int](
      triplet => {
        triplet.dstAttr.foreach(vert =>
          if (triplet.srcAttr contains vert) triplet.sendToSrc(1))
      }, _ + _
    )
    // add outdegree and edge counts to get total number of edges in each egonet
    val edgeCount: RDD[(VertexId, Int)] = outDeg.join(ec).map {
      case (v, (f, s)) => (v, f + s)
    }


    // TOTAL WEIGHTS
    // the weights of outgoing edges of each ego
    val outWeights = egoGraph.aggregateMessages[Double](
      triplet => triplet.sendToSrc(triplet.attr),
      _ + _
    )

    // store outgoing edge weights in each vertex
    val edgeWeightsInVerts: VertexRDD[Array[(VertexId, Double)]] =
      egoGraph.aggregateMessages[Array[(VertexId, Double)]](
        triplet => triplet.sendToSrc(Array((triplet.dstId, triplet.attr))),
        _ ++ _
      )

    // get the weights of edges in egonet not stemming from ego
    val egoPlusWeights = Graph(edgeWeightsInVerts, knnEdges)
    val surroundingWeights = egoPlusWeights.aggregateMessages[Double](
      triplet => {
        triplet.dstAttr.foreach { case (v1, wgt1) =>
          triplet.srcAttr.foreach { case (v2, _) =>
            if (v1 == v2) triplet.sendToSrc(wgt1)
          }
        }
      },
      _ + _
    )

    // combine weights and outdegrees for total weight of each egonet
    val totalWeights: RDD[(VertexId, Double)] = outWeights.join(surroundingWeights).map {
      case (v, (w1, w2)) => (v, w1 + w2)
    }.cache()



    // ===== Comparisons of Graph Features =====

    val labels = vertLabels.join(edgeCount).cache()

    // Weights v. EdgeCounts
    val weightsvEdgeCounts: RDD[(Int, Double, String)] = labels.join(totalWeights).map {
      case (id, ((vType, eCount), tWeight)) => (eCount, tWeight, vType)
    }

    // pRanks v. EdgeCounts
    val pRanksvEdgeCounts = labels.join(pRankEgonet).map {
      case (id, ((vType, eCount), rank)) => (eCount, rank, vType)
    }

    // pRanks v. Weights
    val wLabels = vertLabels.join(totalWeights)
    val pRanksvWeights: RDD[(Double, Double, String)] = wLabels.join(pRankEgonet).map {
      case (id, ((vType, tWeight), rank)) => (rank, tWeight, vType)
    }

    // write comparison data to text file
    weightsvEdgeCounts.coalesce(1, shuffle = true).saveAsTextFile(args(1) + "WeightvsEdge")
    pRanksvWeights.coalesce(1, shuffle = true).saveAsTextFile(args(1) + "EigenvsWeight")
    pRanksvEdgeCounts.coalesce(1, shuffle = true).saveAsTextFile(args(1) + "EigenvsEdge")



    // stop Spark processes
    sc.stop()
    spark.stop()
  }
}
