import main.scala.{PageRankMap, ResearchApp}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.runtime.Nothing$

object PageRankSpark extends ResearchApp {
  val inputFile = args(0)
  val outputFile = args(1)
  val conf = new SparkConf().setAppName("pagerank").setMaster("local").set("spark.ui.enabled", "true")
  val sc = new SparkContext(conf)


  val iters = 2
  val input = sc.textFile(inputFile)
  //  var ranks = links.mapValues(value => 1.0)
  var ranks: RDD[(String, Double)] = null
  var initialRDD: RDD[(String, List[String])] = null
  if (initialRDD == null) {
    initialRDD = input.map { line =>
      val parts = line.split(":")
      val link = parts(0)
      (link, parts(1).trim.split(" ").toList)
    }
    initialRDD.cache()
  }
  for (i <- 1 to 3) {
    ranks = initialRDD
      .flatMap { entry =>
        val size = entry._2.length
        val rank = PageRankMap.getValue(entry._1)
        entry._2.map(value => (value, rank / size))
      }
      .reduceByKey(_ + _)
      .map { pair =>
        val rank = pair._2
        PageRankMap.addResult(pair._1.toString, rank)
        (pair._1, rank)
      }
    ranks.collect()

  }
  ranks.saveAsTextFile(outputFile)
//  System.in.read()
//  dataset1 - 15.698s
  // dataset2 - 36.162s
  // dataset3 - 23.937s
}
