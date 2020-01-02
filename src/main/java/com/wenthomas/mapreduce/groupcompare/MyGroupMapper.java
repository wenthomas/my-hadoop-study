package com.wenthomas.mapreduce.groupcompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-02 18:15
 */
public class MyGroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean k = new OrderBean();

    private NullWritable v = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        //给每行数据封装成一个bean对象输出
        String[] strings = value.toString().split("\t");
        k.setOrderId(strings[0]);
        k.setPid(strings[1]);
        k.setAmount(Double.parseDouble(strings[2]));
        context.write(k, v);
    }

}
