package com.wenthomas.mapreduce.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 19:56
 */

/*
 * 1.输入
 * 		atguigu,a.txt\t3
 * 		atguigu,b.txt\t3
 *
 * 2.输出
 * 		atguigu,a.txt\t3 b.txt\t3
 *
 */
public class MyIDReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer stringBuffer = new StringBuffer();

        for (Text value : values) {
            String[] split = value.toString().split("\t");
            stringBuffer.append(split[0]).append("-->").append(split[1]).append("\t");
        }

        k.set(key);
        v.set(stringBuffer.toString());
        context.write(k, v);
    }
}
