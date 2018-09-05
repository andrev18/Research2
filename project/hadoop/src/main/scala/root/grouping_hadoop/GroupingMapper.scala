package root.grouping_hadoop

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class GroupingMapper extends Mapper[LongWritable, Text, Text, Text] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit ={
    val line = value.toString
    val columns = line.split(",")
    // Filter wrong lines
    if(columns.length == 7) {
      val key = new Text(columns(6))
      context.write(key, value)
    }
  }
}
