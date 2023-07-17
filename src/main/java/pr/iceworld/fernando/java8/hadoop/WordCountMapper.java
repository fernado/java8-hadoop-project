package pr.iceworld.fernando.java8.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/** 词频统计
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN , map 阶段输入key的类型: LongWritable
 * VALUEIN, map 阶段输入value的类型: Text
 * KEYOUT, map 阶段输出vkey的类型：Text
 * VALUEOUT, map阶段输出的value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 放在上面声明防止在循环里多次创建对象，浪费空间
    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();
        // 2. 切割
        String[] words = line.split(" ");
        // 3. 循环写出
        for (String word : words) {
            // 封装 outKey
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}

