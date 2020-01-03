package com.wenthomas.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 14:35
 */


/**
 * 自定义OutputFormat的使用：
 * 1 - 编写Mapper类、Reducer类：由于此案例不需要合并或者排序处理，所以我们不需要Reducer
 * 2 - 编写自定义OutputFormat继承FileOutputFormat
 * 3 - 编写创建自定义RecordWriter继承RecordWriter
 * 4 - 编写Driver：设置自定义输出格式、设置reduceTask数量为0
 */
public class MyOutPutFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\log.txt");
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
        job.setMapperClass(MyOFMapper.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 取消Reducer设置
        job.setNumReduceTasks(0);

        // 设置输出格式
        job.setOutputFormatClass(MyOutputFormat.class);

        // 设置输入目录和输出目录
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // ③运行Job
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0: 1);
    }
}
