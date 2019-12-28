package com.wenthomas.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Verno
 * @create 2019-12-28 10:42
 */

/*
 * 1. Reducer需要复合Hadoop的Reducer规范
 *
 * 2. KEYIN, VALUEIN: Mapper输出的keyout-valueout
 * 		KEYOUT, VALUEOUT: 自定义
 *
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //1, 累加求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //2, 输出
        v.set(sum);
        context.write(key, v);
    }
}
