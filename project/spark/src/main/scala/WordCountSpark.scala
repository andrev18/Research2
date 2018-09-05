import main.scala.ResearchApp
import org.apache.spark.{SparkConf, SparkContext}

object WordCountSpark extends ResearchApp {

  val inputFile = args(0)
  val outputFile = args(1)
  val conf = new SparkConf().setAppName("wordCount").setMaster("local").set("spark.ui.enabled","true")
  // Create a Scala Spark Context.
  val sc = new SparkContext(conf)
  // Load our input data.
  val input =  sc.textFile(inputFile)
  val input2 =  sc.textFile(inputFile)
  input.flatMap(line => line.split(" "))
    .map(word => (word,1))
    .reduceByKey{case (x, y) => x + y}
    .saveAsTextFile(outputFile)
  System.in.read()
  //21.669 - 500mb
  //5.095 - 100mb
  //3.170- 50mb
}
