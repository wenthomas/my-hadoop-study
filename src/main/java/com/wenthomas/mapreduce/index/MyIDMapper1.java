package com.wenthomas.mapreduce.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 19:33
 */

/*
 * 1.输入
 * 		atguigu pingping
 * 2.输出
 * 		atguigu-a.txt,1
 */
public class MyIDMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    String source;

    /**
     * 在调用map()方法之前加载当前切片所属文件
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        source = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(" ");
        for (String string : strings) {
            k.set(string.toString() + ":" + source);
            context.write(k, v);
        }
    }
}
