package root

import main.scala.ResearchApp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import root.grouping_hadoop.{GroupingMapper, GroupingReducer}

object GroupingHadoop extends ResearchApp {
  util.Properties.setProp("scala.time", "true")

    val conf = new Configuration
    val job = new Job(conf, "grouping")

    job.setJarByClass(this.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setMapperClass(classOf[GroupingMapper])
    job.setReducerClass(classOf[GroupingReducer])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    job.setNumReduceTasks(1)

    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    val success = job.waitForCompletion(true)
    System.out.println(success)

}
