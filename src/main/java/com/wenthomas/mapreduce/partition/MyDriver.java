package com.wenthomas.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-28 17:37
 */

/**
 * 自定义分区案例实现：（修改自手机流量统计案例）
 * 1 - Driver类中设置job.setNumReduceTask() 分区数 以及指定分区器job.setPartitionerClass()
 * 2 - 编写自定义分区器继承Partitioner，并重写getPartition() 方法制定新的分区规则
 */
public class MyDriver {
    private final static Logger log = LoggerFactory.getLogger(MyDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\phone_data.txt");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\output");

        //作为整个Job的配置
        Configuration conf = new Configuration();

        //保证输出目录不存在
        FileSystem fs= FileSystem.get(conf);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

        }

        // ①创建Job
        Job job = Job.getInstance(conf);

        // ②设置Job
        // 设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置分区数为5
        job.setNumReduceTasks(5);
        // 指定分区器
        job.setPartitionerClass(MyPartitioner.class);

        // ③运行Job
        job.waitForCompletion(true);
    }
}
