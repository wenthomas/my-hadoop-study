package com.wenthomas.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-01-03 18:39
 */

/*
 * 1. 在Hadoop中，hadoop为MR提供了分布式缓存
 * 			①用来缓存一些Job运行期间的需要的文件(普通文件，jar，归档文件(har))
 * 			②通过在Job的Configuration中，使用uri代替要缓存的文件
 * 			③分布式缓存会假设当前的文件已经上传到了HDFS，并且在集群的任意一台机器都可以访问到这个URI所代表的文件
 * 			④分布式缓存会在每个节点的task运行之前，提前将文件发送到节点
 * 			⑤分布式缓存的高效是由于每个Job只会复制一次文件，且可以自动在从节点对归档文件解归档
 */
public class MyMJMapper extends Mapper<LongWritable, Text, MyJBean, NullWritable> {

    private MyJBean k = new MyJBean();
    private NullWritable v = NullWritable.get();

    //存放较小关联表（pd.txt）读到的所有数据
    private Map<String, Object> pdMap = new HashMap<>();

    //在map()方法之前加载较小的关联表
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //从分布式缓存中读取数据
        URI[] cacheFiles = context.getCacheFiles();
        for (URI cacheFile : cacheFiles) {
            //通过BufferReader来按行读取文件内容
            BufferedReader reader = new BufferedReader(new FileReader(new File(cacheFile)));
            String line = "";
            //循环读取pd.txt中的每一行
            while (StringUtils.isNotBlank(line = reader.readLine())) {
                //切割
                String[] fields = line.split("\t");
                //缓存数据到集合
                pdMap.put(fields[0], fields[1]);
            }
            //关流
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //执行join操作
        String[] strings = value.toString().split("\t");
        k.setOrderId(strings[0]);
        k.setPname(pdMap.get(strings[1]).toString());
        k.setAmount(strings[2]);
        k.setPid(strings[1]);

        context.write(k, v);
    }
}
