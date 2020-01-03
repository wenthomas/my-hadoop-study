package com.wenthomas.mapreduce.mapjoin;

import com.wenthomas.mapreduce.reducejoin.MyJBean;
import com.wenthomas.mapreduce.reducejoin.MyRJMapper;
import com.wenthomas.mapreduce.reducejoin.MyRJPartitioner;
import com.wenthomas.mapreduce.reducejoin.MyRJReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Verno
 * @create 2020-01-03 19:06
 */

/**
 * 案例：Map Join的应用
 * 1 - 编写Mapper：
 *          在调用map()方法前加载setup()方法读取分布式缓存文件
 *          通过BufferedReader按行读取预先封装好较小关联表中的对象
 *          在map()方法中直接完成join操作
 *          由于Map阶段已join好所需要的数据，所以不再需要Reducer
 * 2 - 编写Bean
 * 3 - 编写Driver：
 *          创建Job实例
 *          设置添加分布式缓存文件：job.addCacheFile()
 *          取消reduce阶段设置
 */
public class MyMJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Path inputPath=new Path("E:\\1015\\my_data\\mr\\map_join\\order.txt");
        Path outputPath=new Path("E:\\1015\\my_data\\mr\\map_join\\output");

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
        job.setMapperClass(MyMJMapper.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setMapOutputKeyClass(com.wenthomas.mapreduce.mapjoin.MyJBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 取消reduce阶段
        job.setNumReduceTasks(0);

        //设置分布式缓存
        //注意：本地文件系统需要加上协议file:///
        job.addCacheFile(new URI("file:///e:/1015/my_data/mr/map_join/pd.txt"));

        // ③运行Job
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0: 1);
    }
}
