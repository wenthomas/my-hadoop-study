package com.wenthomas.mapreduce.seekfriends.lesson;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * keyin-valuein : （友:用户）
					（c:A）,(C:B),(C:E)
	reduce(): 	
	keyout-valueout  ：（友：用户，用户，用户，用户）
 */
public class Example3Reducer extends Reducer<Text, Text, Text, Text>{
	
	private Text out_value=new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for (Text text : value) {
			
			sb.append(text.toString()+",");
		}
		
		out_value.set(sb.toString());
		
		context.write(key, out_value);
		
		
		
	}

}
