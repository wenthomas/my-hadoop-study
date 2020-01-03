package com.wenthomas.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 11:04
 */
public class MyOFMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private NullWritable v = NullWritable.get();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        k.set(string);
        context.write(k, v);
    }
}
