package com.wenthomas.mapreduce.seekfriends;

import com.wenthomas.mapreduce.flowcount.FlowBean;
import com.wenthomas.mapreduce.flowcount.MyMapper;
import com.wenthomas.mapreduce.flowcount.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 21:26
 */
public class MySFDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\seekfriends");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\seekfriends\\output");

        //作为整个Job的配置
        Configuration conf = new Configuration();
        //设置key-value的分隔符
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");

        //保证输出目录不存在
        FileSystem fs= FileSystem.get(conf);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

        }

        // ①创建Job
        Job job = Job.getInstance(conf);

        // ②设置Job
        // 设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job.setMapperClass(MySFMapper1.class);
        job.setReducerClass(MySFReducer1.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // ③运行Job
        job.waitForCompletion(true);
    }
}
