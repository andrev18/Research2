package root.word_count_hadoop

import java.util.StringTokenizer

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val one: IntWritable = new IntWritable(1)
  val word: Text = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit ={
    val line = value.toString
    val tokenizer = new StringTokenizer(line)
    while ( {
      tokenizer.hasMoreTokens
    }) {
      word.set(tokenizer.nextToken)
      context.write(word, one)
    }
  }
}
