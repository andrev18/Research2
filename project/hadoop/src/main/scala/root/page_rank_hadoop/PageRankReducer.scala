package root.page_rank_hadoop

import java.lang

import main.scala.PageRankMap
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class PageRankReducer extends Reducer[Text, DoubleWritable, Text, DoubleWritable] {
  override def reduce(key: Text, values: lang.Iterable[DoubleWritable], context: Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {
    var sum = 0d
    import scala.collection.JavaConversions._
    for (value <- values) {
      sum += value.get
    }
    PageRankMap.addResult(key.toString, sum)
    context.write(key, new DoubleWritable(sum))
  }
}
