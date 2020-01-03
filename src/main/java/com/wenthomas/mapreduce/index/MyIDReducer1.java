package com.wenthomas.mapreduce.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 19:40
 */

/*
 * 1.输入
 * 		atguigu-a.txt,1
 * 		atguigu-a.txt,1
 * 		atguigu-a.txt,1
 * 2.输出
 * 		atguigu-a.txt,3
 */
public class MyIDReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += 1;
        }
        context.write(key, new IntWritable(sum));
    }
}
