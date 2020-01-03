package com.wenthomas.mapreduce.reducejoin;

import com.wenthomas.mapreduce.partition.FlowBean;
import com.wenthomas.mapreduce.partition.MyMapper;
import com.wenthomas.mapreduce.partition.MyPartitioner;
import com.wenthomas.mapreduce.partition.MyReducer;
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
 * @create 2020-01-03 18:17
 */

/**
 * 案例：Reduce Join的应用
 * ① 编写Mapper
 * ② 编写自定义分区器:保证pid相同的k-v分到同一个区
 * ③ 编写Bean类
 * ④ 编写Reducer:根据数据来源进行分类、重写cleanup() 方法来完成join操作
 * ⑤ 编写Driver：设置Job的分区器
 */
public class MyRJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\reduce_join");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\reduce_join\\output");

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
        job.setMapperClass(MyRJMapper.class);
        job.setReducerClass(MyRJReducer.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MyJBean.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 指定分区器
        job.setPartitionerClass(MyRJPartitioner.class);

        // ③运行Job
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0: 1);
    }
}
