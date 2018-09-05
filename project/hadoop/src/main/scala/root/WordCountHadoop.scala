package root

import main.scala.ResearchApp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import root.word_count_hadoop.{WordCountMapper, WordCountReducer}

object WordCountHadoop extends ResearchApp {
  util.Properties.setProp("scala.time", "true")
  val conf = new Configuration
  val job = new Job(conf, "wordcount")

  job.setJarByClass(this.getClass)
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])
  job.setMapperClass(classOf[WordCountMapper])
  job.setReducerClass(classOf[WordCountReducer])
  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
  job.setNumReduceTasks(1)

  FileInputFormat.setInputPaths(job, new Path(args(0)))
  FileOutputFormat.setOutputPath(job, new Path(args(1)))

  val success = job.waitForCompletion(true)
  System.out.println(success)

  //50mb 21.995s
  //100mb 40.257s
  //500mb 200.062s


}
