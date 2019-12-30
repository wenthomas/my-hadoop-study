package com.wenthomas.mapreduce.custominputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-30 18:31
 */
public class MyCustomMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
    //因为RecordReader把整个文件作为value输入，所以不需要再循环输出操作，直接默认输出即可。
}
