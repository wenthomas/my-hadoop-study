package com.wenthomas.mapreduce.groupcompare.lesson;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 10000001	Pdt_01	222.8
10000001	Pdt_02	222.8
10000001	Pdt_05	25.8
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{

	/*
	 * OrderBean key-NullWritable nullWritable在reducer工作期间，
	 * 	只会实例化一个key-value的对象！
	 * 		每次调用迭代器迭代下个记录时，使用反序列化器从文件中或内存中读取下一个key-value数据的值，
	 * 		封装到之前OrderBean key-NullWritable nullWritable在reducer的属性中
	 */
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		Double maxAcount = key.getAcount();
		
		for (NullWritable nullWritable : values) {
			
			if (!key.getAcount().equals(maxAcount)) {
				break;
			}
			//复合条件的记录
			context.write(key, nullWritable);
			
		}
		
	}
	
}
