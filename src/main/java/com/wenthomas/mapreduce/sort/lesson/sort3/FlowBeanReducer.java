package com.wenthomas.mapreduce.sort.lesson.sort3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowBeanReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			
			context.write(value, key);
			
		}
		
	}
	
	
}
