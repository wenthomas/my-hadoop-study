package com.wenthomas.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-28 16:19
 */

/*
 * 1. 统计手机号(String)的上行(long,int)，下行(long,int)，总流量(long,int)
 *
 * 手机号为key,Bean{上行(long,int)，下行(long,int)，总流量(long,int)}为value
 *
 *
 *
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text v = new Text();

    private FlowBean k = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //获取一行
        String[] fields = value.toString().split("\t");
        //封装对象
        String phoneNum = fields[0];
        long upflow = Long.parseLong(fields[1]);
        long downflow = Long.parseLong(fields[2]);
        k.setUpFlow(upflow);
        k.setDownFlow(downflow);
        k.setSumFlow(upflow + downflow);
        v.set(phoneNum);

        //以Bean作为key,比较时比较的是sumFlow字段
        context.write(k, v);
    }
}
