package pr.iceworld.fernando.java8.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class WordCountDriver{
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取 job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2. 设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        // 3. 关联 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4. 设置 map 输出的 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 设置最终输出的k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("java8-hadoop/testInput"), new Path("java8-hadoop/testInput2"));
        File path = new File("java8-hadoop/testOutput");
        if (path.exists()) {
            File[] files = path.listFiles();
            Iterator<File> iterator = Arrays.stream(files).iterator();
            while(iterator.hasNext()) {
                iterator.next().delete();
            }
            path.delete();
        }
        FileOutputFormat.setOutputPath(job, new Path("java8-hadoop/testOutput"));
        // 7. 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

