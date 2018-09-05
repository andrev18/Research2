import PageRankFormatterSpark.args
import main.scala.ResearchApp
import org.apache.spark.{SparkConf, SparkContext}

object GroupingSpark extends ResearchApp {
  val inputFile = args(0)
  val outputFile = args(1)
  val conf = new SparkConf().setAppName("grouping").setMaster("local").set("spark.ui.enabled", "true")
  val sc = new SparkContext(conf)
  val input = sc.textFile(inputFile)

  input
    .map(line => line.toString.split(","))
    .filter(result => result.length == 7)
    .map(result => (result(6), result.mkString)) // group by column 6
    .groupByKey()
    .saveAsTextFile(outputFile)
}
