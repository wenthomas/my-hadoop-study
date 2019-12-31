package com.wenthomas.mapreduce.partition;

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
public class MyMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text k = new Text();

    private FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //封装手机号
        String[] strings = value.toString().split("\t");
        String string = strings[1];
        k.set(string);

        //封装上行
        v.setUpFlow(Long.parseLong(strings[strings.length - 3]));
        //封装下行
        v.setDownFlow(Long.parseLong(strings[strings.length - 2]));

        context.write(k, v);
    }
}
