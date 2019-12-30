package com.wenthomas.mapreduce.custominputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Verno
 * @create 2019-12-30 19:55
 */
public class MyCustomReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

}
