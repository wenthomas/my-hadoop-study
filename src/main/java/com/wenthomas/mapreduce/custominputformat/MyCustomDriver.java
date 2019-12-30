package com.wenthomas.mapreduce.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-30 19:58
 */
public class MyCustomDriver{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inputPath=new Path("E:\\1015\\my_data\\mr\\custom");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\custom\\output");

        //1-创建Job实例
        Configuration configuration = new Configuration();
        //保证输出目录不存在
        FileSystem fs=FileSystem.get(configuration);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyCustomDriver.class);

        //2-设置Job参数
        job.setMapperClass(MyCustomMapper.class);
        job.setReducerClass(MyCustomReducer.class);
        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //设置MR输入输出格式
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //设置输入输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //3-提交Job任务
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0: 1);
    }
}
