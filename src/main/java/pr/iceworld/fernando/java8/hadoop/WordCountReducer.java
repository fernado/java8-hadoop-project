package pr.iceworld.fernando.java8.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** 词频统计
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN , reduce 阶段输入key的类型: Text
 * VALUEIN, reduce 阶段输入value的类型: IntWritable
 * KEYOUT, reduce 阶段输出vkey的类型：Text
 * VALUEOUT, reduce阶段输出的value类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private  IntWritable outValue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        outValue.set(sum);
        // 写出
        context.write(key, outValue);
    }
}

