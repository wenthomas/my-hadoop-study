package com.wenthomas.mapreduce.sort.lesson.sort1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowBeanReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			
			context.write(value, key);
			
		}
		
	}
	
	
}
