package com.wenthomas.mapreduce.groupcompare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-02 18:39
 */

/*
 * 1.一旦启动这个线程，运行Job
 *
 * 2.本地模式主要用于测试程序是否正确！
 *
 * 3. 报错：
 * 	ExitCodeException exitCode=1: /bin/bash: line 0: fg: no job control
 */
public class MyGroupDriver {



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\GroupingComparator.txt");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\output");

        //作为整个Job的配置
        Configuration conf = new Configuration();

        //保证输出目录不存在
        FileSystem fs=FileSystem.get(conf);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

        }

        // ①创建Job
        Job job = Job.getInstance(conf);

        // ②设置Job
        // 设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job.setMapperClass(MyGroupMapper.class);
        job.setReducerClass(MyGroupReducer.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置自定义的分组比较器
        job.setGroupingComparatorClass(MyComparator1.class);

        // ③运行Job
        job.waitForCompletion(true);
    }
}
