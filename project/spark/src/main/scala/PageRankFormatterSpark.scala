import PageRankSpark.args
import main.scala.ResearchApp
import org.apache.spark.{SparkConf, SparkContext}

object PageRankFormatterSpark extends ResearchApp {
  val inputFile = args(0)
  val outputFile = args(1)
  val conf = new SparkConf().setAppName("pagerank").setMaster("local").set("spark.ui.enabled", "true")
  val sc = new SparkContext(conf)
  val input = sc.textFile(inputFile)

  input
    .map(line => line.toString.split(" "))
    .map(result => (Integer.parseInt(result(0)), result(1)))
    .reduceByKey(func = (x, y) => (x + " " + y), 1)
    .sortByKey(true, 1)
    .saveAsTextFile(outputFile)

}
