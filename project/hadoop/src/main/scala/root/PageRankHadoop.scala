package root

import main.scala.ResearchApp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import root.page_rank_hadoop.{PageRankMapper, PageRankReducer}

import scala.io.Source

object PageRankHadoop extends ResearchApp {
  util.Properties.setProp("scala.time", "true")
  for (iteration <- 1 to 4) {
    val success = generateJob(iteration).waitForCompletion(true)
 }

  def generateJob(iteration: Int): Job = {
    val conf = new Configuration
    val job = Job.getInstance(conf)

    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])
    job.setMapperClass(classOf[PageRankMapper])
    job.setReducerClass(classOf[PageRankReducer])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    job.setNumReduceTasks(1)

    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1) + "/iteration_" + iteration))
    job
  }

  //dataset1 - 18.157s
  //dataset2 - 40.371s
  //dataset3 - 91.651s
}
