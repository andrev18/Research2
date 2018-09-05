package root.word_count_hadoop

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer extends Reducer[Text,IntWritable,Text,IntWritable]{
  val value: IntWritable = new IntWritable(0)

  override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit ={
    var sum = 0
    import scala.collection.JavaConversions._
    for (value <- values) {
      sum += value.get
    }
    value.set(sum)
    context.write(key, value)
  }
}
