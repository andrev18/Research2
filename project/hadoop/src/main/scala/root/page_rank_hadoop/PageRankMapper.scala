package root.page_rank_hadoop

import main.scala.PageRankMap
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper

class PageRankMapper extends Mapper[LongWritable, Text, Text, DoubleWritable] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, DoubleWritable]#Context): Unit = {
    val line = value.toString
    val parts = line.split(":")
    val key = new Text(parts(0))
    val links = parts(1).trim().split(" ")
    val size = links.length
    var rank = PageRankMap.getValue(parts(0))


    links.foreach(link =>
      context.write(new Text(link), new DoubleWritable(rank / size))
    )
  }
}


