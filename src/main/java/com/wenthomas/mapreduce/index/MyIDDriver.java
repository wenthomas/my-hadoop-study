package com.wenthomas.mapreduce.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Verno
 * @create 2020-01-03 19:42
 */

/**
 * 倒排索引案例（多job串联）
 * 1 - 创建Job1:
 *          编写Mapper1、Reducer1
 * 2 - 创建Job2：
 *          编写Mapper2：可使用KeyValueTextInputFormat按分割符读入数据，则不需要重写Mapper方法
 *          编写Reducer2:
 * 3 - 编写Driver：
 *          分别定义Job1、Job2
 *          设置Job1、Job2：Job1的输出路径为Job2的输入路径，Job2引用KeyValueTextInputFormat配置并设置分隔符属性
 *          构建JobControl：添加ControlledJob列表并设置Job2对Job1的依赖关系
 *          运行JobControl（新开线程运行Job组）
 *          设置守护线程
 *          获取JobControl线程的运行状态，并判断整个jobControl是否全部运行结束
 *
 *
 *
 */
public class MyIDDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\index");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\index\\output");
        Path finalOutputPath=new Path("E:\\1015\\my_data\\mr\\index\\finaloutput");

        //作为整个Job的配置
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        //设置key-value的分隔符
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");

        //保证输出目录不存在
        FileSystem fs= FileSystem.get(conf1);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

        }
        if (fs.exists(finalOutputPath)) {

            fs.delete(finalOutputPath, true);

        }

        // ①创建Job
        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);

        // ②设置Job
        // 设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job1.setMapperClass(MyIDMapper1.class);
        job1.setReducerClass(MyIDReducer1.class);

        job2.setMapperClass(MyIDMapper2.class);
        job2.setReducerClass(MyIDReducer2.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // 设置job2的输入格式
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //--------------------------------------------------------
        //构建JobControl
        //创建运行的Job
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        List<ControlledJob> jobList = new ArrayList<>();
        jobList.add(controlledJob1);
        jobList.add(controlledJob2);
        JobControl jobControl = new JobControl("index");
        // 向jobControl设置要运行哪些job
        jobControl.addJobCollection(jobList);
        //指定依赖关系
        controlledJob2.addDependingJob(controlledJob1);
        //运行JobControl
        Thread thread = new Thread(jobControl);
        //设置此线程为守护线程
        thread.setDaemon(true);
        thread.start();

        //获取JobControl线程的运行状态
        while (true) {
            //判断整个jobControl是否全部运行结束
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                return;
            }
        }
    }
}
