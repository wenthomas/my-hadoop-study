package com.wenthomas.mapreduce.outputformat.lesson;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 1.什么时候需要Reduce
 * 		①合并
 * 		②需要对数据排序
 * 
 * 2. 没有Reduce阶段，key-value不需要实现序列化
 */
public class CustomOFMapper extends Mapper<LongWritable, Text, String, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		String content = value.toString();
		
		context.write(content+"\r\n", NullWritable.get());
		
	}

}
