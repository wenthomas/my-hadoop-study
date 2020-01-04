package com.wenthomas.mapreduce.seekfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * @create 2020-01-03 21:26
 */

/**
 * 找博客共同好友案例:
 * 1 - 创建Job1：
 *              编写Mapper1
 *              编写Reducer1
 * 2 = 创建Job2：
 *              编写Mapper2：两两遍历共同好友作为Key，并需要保持数组的顺序
 *              编写Reducer2
 * 3 - 编写Driver：
 *              定义两个Job
 *              配置两个Job的设置
 *              创建并运行Job组
 */
public class MySFDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\seekfriends");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\seekfriends\\output");
        Path finalOutputPath=new Path("E:\\1015\\my_data\\mr\\seekfriends\\finaloutput");

        //作为整个Job的配置
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        //设置key-value的分隔符
        conf1.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");

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
        job1.setMapperClass(MySFMapper1.class);
        job1.setReducerClass(MySFReducer1.class);

        job2.setMapperClass(MySFMapper2.class);
        job2.setReducerClass(MySFReducer2.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // 设置job的输入格式
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //--------------------------------------------------------
        //构建JobControl
        //创建运行的Job
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        List<ControlledJob> jobList = new ArrayList<>();
        jobList.add(controlledJob1);
        jobList.add(controlledJob2);
        JobControl jobControl = new JobControl("seekFriends");
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
