package com.wenthomas.mapreduce.wordcount;

import com.wenthomas.hdfs.download.MyDownloadPartly;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Verno
 * @create 2019-12-28 10:23
 */
public class MyWordCount {

    private final static Logger log = LoggerFactory.getLogger(MyWordCount.class);

    /**
     * hadoop集群运行MR任务
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //运行报权限问题时，需要加上以下系统变量，以设置Hadoop用户变量
        System.setProperty("HADOOP_USER_NAME", "atguigu");

/*        String inputPath = "E:\\1015\\my_data\\mr\\hello.txt";
        String outputPath = "E:\\1015\\my_data\\mr\\output";*/

        String inputPath = "/mr/chapter01/hello.txt";
        String outputPath = "/mr/chapter01/output";

        //1, 获取Job配置信息以及封装任务
        //configuration无配置则表示使用本地模式运行任务
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop01:9000");

        //todo: 获取文件系统并删除output目录
        FileSystem fileSystem = FileSystem.get(configuration);

        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

        //2, 获取Job实例
        Job job = Job.getInstance(configuration);

        //3, 设置Job参数
        job.setJarByClass(MyWordCount.class);
        //设置Mapper和Reducer类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setJarByClass(MyWordCount.class);

        //4, 提交Job任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0: 1);
    }
}
