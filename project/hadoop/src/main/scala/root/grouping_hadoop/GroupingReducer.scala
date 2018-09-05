package root.grouping_hadoop

import java.lang
import java.util.function.Consumer

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class GroupingReducer extends Reducer[Text, Text, Text, Text] {

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    var result = ""

    values.forEach {
      new Consumer[Text] {
        override def accept(value: Text): Unit = {
          if (!result.isEmpty) {
            result = result + " / "
          }
          result = result + value
        }
      }
    }

    context.write(key, new Text(result))
  }
}