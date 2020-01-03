package com.wenthomas.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 15:48
 */

/*
 * Map阶段无法完成Join，只能封装数据，在Reduce阶段完成Join
 *
 * 1. order.txt: 1001	01	1
 * 	 pd.txt :  01 小米
 *
 * 2. Bean必须能封装所有的数据
 *
 * 3. Reduce只需要输出来自于order.txt的数据，需要在Mapper中对数据打标记，标记数据的来源
 *
 * 4. 在Mapper中需要获取当前切片的来源，根据来源执行不同的封装逻辑
 */
public class MyRJMapper extends Mapper<LongWritable, Text, NullWritable, MyJBean> {

    private NullWritable k = NullWritable.get();
    private MyJBean v = new MyJBean();
    private String source;

    /**
     * 默认同一切片的数据来源于同一文件，可从split中获取到当前文件名
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit split = (FileSplit) inputSplit;
        source = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");
        //判断切片来源于哪一个文件
        if (source.equals("order.txt")) {
            v.setOrderId(strings[0]);
            v.setPid(strings[1]);
            v.setAmount(strings[2]);
            // 保证所有的属性不为null，否则会报空指针异常
            v.setPname("null");
            v.setSource(source);
        } else if(source.equals("pd.txt")) {
            v.setPid(strings[0]);
            v.setPname(strings[1]);
            // 保证所有的属性不为null，否则会报空指针异常
            v.setOrderId("null");
            v.setAmount("null");
            v.setSource(source);
        } else {
            return;
        }

        context.write(k, v);
    }
}
